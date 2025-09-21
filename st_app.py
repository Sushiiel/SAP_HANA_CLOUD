# st_app.py
import streamlit as st
import socket
import traceback
import time
from datetime import datetime

# If using proxy mode:
import requests

# Optional direct DB driver (only used if proxy not configured)
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None  # will raise if direct DB mode attempted but driver missing

SCHEMA_NAME = "SMART_RETAIL1"

# ----------------- Configuration / Mode detection -----------------
USE_PROXY = False
PROXY_URL = None
PROXY_KEY = None
HEADERS = {}

# Try to read proxy secrets first (Streamlit Cloud)
try:
    proxy_conf = st.secrets.get("proxy", None)
    if proxy_conf and proxy_conf.get("url") and proxy_conf.get("api_key"):
        PROXY_URL = proxy_conf["url"].rstrip("/")
        PROXY_KEY = proxy_conf["api_key"]
        HEADERS = {"X-API-KEY": PROXY_KEY}
        USE_PROXY = True
except Exception:
    USE_PROXY = False

# If proxy not configured, try to read HANA secrets for direct DB access
hana_config = None
if not USE_PROXY:
    try:
        hana_secrets = st.secrets.get("hana", None)
        if hana_secrets:
            hana_config = {
                "address": hana_secrets["address"],
                "port": int(hana_secrets.get("port", 443)),
                "user": hana_secrets["user"],
                "password": hana_secrets["password"],
                "encrypt": hana_secrets.get("encrypt", True),
                "sslValidateCertificate": hana_secrets.get("sslValidateCertificate", False),
            }
    except Exception:
        hana_config = None

# ----------------- Direct HANA connection helper (only used if proxy not configured) -----------------
def _get_direct_connection(retries: int = 3, backoff_s: float = 2.0):
    if not hana_config:
        raise RuntimeError("HANA configuration not found in secrets and proxy not configured.")
    if dbapi is None:
        raise RuntimeError("hdbcli driver not available in environment. Use proxy or deploy where hdbcli is installable.")
    host = hana_config["address"]
    port = hana_config["port"]

    # quick tcp check
    try:
        with socket.create_connection((host, port), timeout=8):
            pass
    except Exception as e:
        raise ConnectionError(f"Network check failed reaching {host}:{port} — {e}")

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(**hana_config)
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(backoff_s * attempt)
    tb = traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__)
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{''.join(tb)}")

# If using direct DB, establish connection once and reuse via st.cache_resource-like behavior
conn = None
cursor = None
if not USE_PROXY:
    try:
        conn = _get_direct_connection()
        cursor = conn.cursor()
    except Exception as e:
        st.error("Direct HANA DB connection failed. Either configure proxy or ensure the app runs where HANA is reachable.")
        st.exception(e)
        st.stop()

# ----------------- Proxy wrappers -----------------
def _proxy_get_products():
    if not PROXY_URL:
        raise RuntimeError("Proxy not configured (st.secrets['proxy']).")
    r = requests.get(f"{PROXY_URL}/products", headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json().get("products", [])

def _proxy_insert_product(name: str, description: str):
    r = requests.post(f"{PROXY_URL}/product", headers=HEADERS, json={"name": name, "description": description}, timeout=15)
    r.raise_for_status()
    return r.json()

def _proxy_update_product(name: str, description: str):
    # use requests.utils.requote_uri for safe names
    uri_name = requests.utils.requote_uri(name)
    r = requests.put(f"{PROXY_URL}/product/{uri_name}", headers=HEADERS, json={"description": description}, timeout=15)
    r.raise_for_status()
    return r.json()

def _proxy_delete_product(name: str):
    uri_name = requests.utils.requote_uri(name)
    r = requests.delete(f"{PROXY_URL}/product/{uri_name}", headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()

# ----------------- DB-like helper functions (same public API as before) -----------------
def fetch_product_names():
    if USE_PROXY:
        try:
            products = _proxy_get_products()
            return [p["name"] for p in products]
        except Exception as e:
            st.error("Failed to fetch product list from proxy.")
            st.exception(e)
            return []
    else:
        try:
            cursor.execute(f'SELECT DISTINCT NAME FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
            rows = cursor.fetchall()
            return [r[0] for r in rows] if rows else []
        except Exception as e:
            st.error("Direct DB: failed to fetch product names.")
            st.exception(e)
            return []

def get_product_description(name):
    if USE_PROXY:
        try:
            products = _proxy_get_products()
            for p in products:
                if p["name"] == name:
                    return p.get("description", "No description")
            return "No description found."
        except Exception as e:
            st.error("Failed to fetch product description from proxy.")
            st.exception(e)
            return "Error fetching description."
    else:
        try:
            cursor.execute(f'SELECT DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
            row = cursor.fetchone()
            return row[0] if row else "No description found."
        except Exception as e:
            st.error("Direct DB: failed to fetch product description.")
            st.exception(e)
            return "Error fetching description."

def insert_product(name, description):
    if USE_PROXY:
        try:
            return _proxy_insert_product(name, description)
        except Exception as e:
            st.error("Proxy insert failed.")
            st.exception(e)
            return None
    else:
        try:
            cursor.execute(f'SELECT MAX(PRODUCT_ID) FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
            row = cursor.fetchone()
            max_id = row[0] if row and row[0] is not None else 0
            new_id = max_id + 1
            cursor.execute(
                f'INSERT INTO "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
                (new_id, name, description, None)
            )
            conn.commit()
            return {"status": "ok", "product_id": new_id}
        except Exception as e:
            st.error("Direct DB: insert failed.")
            st.exception(e)
            return None

def update_product_description(name, new_description):
    if USE_PROXY:
        try:
            return _proxy_update_product(name, new_description)
        except Exception as e:
            st.error("Proxy update failed.")
            st.exception(e)
            return None
    else:
        try:
            cursor.execute(
                f'UPDATE "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" SET DESCRIPTION = ? WHERE NAME = ?',
                (new_description, name)
            )
            conn.commit()
            return {"status": "ok", "rows_affected": cursor.rowcount}
        except Exception as e:
            st.error("Direct DB: update failed.")
            st.exception(e)
            return None

def delete_product(name):
    if USE_PROXY:
        try:
            return _proxy_delete_product(name)
        except Exception as e:
            st.error("Proxy delete failed.")
            st.exception(e)
            return None
    else:
        try:
            cursor.execute(f'DELETE FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
            conn.commit()
            return {"status": "ok", "rows_affected": cursor.rowcount}
        except Exception as e:
            st.error("Direct DB: delete failed.")
            st.exception(e)
            return None

def view_products(limit: int = 200):
    if USE_PROXY:
        try:
            products = _proxy_get_products()
            return [(p.get("product_id"), p.get("name"), p.get("description")) for p in products]
        except Exception as e:
            st.error("Failed to list products from proxy.")
            st.exception(e)
            return []
    else:
        try:
            cursor.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID LIMIT {limit}')
            return cursor.fetchall()
        except Exception as e:
            st.error("Direct DB: view products failed.")
            st.exception(e)
            return []

# ----------------- Streamlit UI (unchanged) -----------------
st.sidebar.title("🔍 Select Action")
menu = st.sidebar.selectbox("", ["Product Insights", "Insert Product", "View Products", "Update Product", "Delete Product"])

if menu == "Product Insights":
    st.title("🔎 Product Insights")
    names = fetch_product_names()
    if not names:
        st.warning("No products found in the database.")
    else:
        selected = st.selectbox("Select a product", names)
        if st.button("Explain product"):
            desc = get_product_description(selected)
            st.info(f"Description: {desc}")

elif menu == "Insert Product":
    st.title("➕ Add New Product")
    new_name = st.text_input("Product name")
    new_desc = st.text_area("Product description (optional)")
    if st.button("Insert"):
        if not new_name.strip():
            st.warning("Enter a product name.")
        else:
            description = new_desc.strip() or "AI-generated description placeholder."
            try:
                res = insert_product(new_name.strip(), description)
                if res is None:
                    st.error("Insert did not succeed.")
                else:
                    # res may be dict (proxy or direct): try to present product_id
                    pid = res.get("product_id") if isinstance(res, dict) else None
                    st.success(f"Inserted '{new_name}' with PRODUCT_ID={pid}")
            except Exception as e:
                st.error(f"Insertion failed: {e}")
                st.exception(e)

elif menu == "View Products":
    st.title("📋 All Products")
    rows = view_products(1000)
    if not rows:
        st.warning("No products found.")
    else:
        st.dataframe([{"PRODUCT_ID": r[0], "NAME": r[1], "DESCRIPTION": r[2]} for r in rows])

elif menu == "Update Product":
    st.title("✏️ Update Product Description")
    names = fetch_product_names()
    if not names:
        st.warning("No products to update.")
    else:
        choice = st.selectbox("Product to update", names)
        new_description = st.text_area("New description")
        if st.button("Update"):
            if not new_description.strip():
                st.warning("Enter a new description.")
            else:
                try:
                    res = update_product_description(choice, new_description.strip())
                    if res and res.get("status") == "ok":
                        st.success(f"Updated {res.get('rows_affected', 'N/A')} row(s).")
                    else:
                        st.error(f"Update failed: {res}")
                except Exception as e:
                    st.error(f"Update failed: {e}")
                    st.exception(e)

elif menu == "Delete Product":
    st.title("🗑️ Delete Product")
    names = fetch_product_names()
    if not names:
        st.warning("No products to delete.")
    else:
        choice = st.selectbox("Product to delete", names)
        if st.button("Delete"):
            try:
                res = delete_product(choice)
                if res and res.get("status") == "ok":
                    st.success(f"Deleted {res.get('rows_affected', 'N/A')} row(s).")
                else:
                    st.error(f"Delete failed: {res}")
            except Exception as e:
                st.error(f"Delete failed: {e}")
                st.exception(e)

# optional close
def _close_conn():
    try:
        if not USE_PROXY and cursor:
            cursor.close()
        if not USE_PROXY and conn:
            conn.close()
    except Exception:
        pass

st.button("Close DB Connection (optional)", on_click=_close_conn)

# Diagnostics expander
with st.expander("Diagnostics / Debug"):
    st.write("Mode: Proxy" if USE_PROXY else "Mode: Direct HANA")
    st.write("Proxy URL:", PROXY_URL or "not configured")
    st.write("Proxy key present:", bool(PROXY_KEY))
    try:
        st.write("Local timestamp:", datetime.now().isoformat())
    except Exception:
        pass
