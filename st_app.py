# st_app.py (updated)
import os
import socket
import time
import traceback
from datetime import datetime

import streamlit as st

# try to import hdbcli; we'll surface a helpful message if it's not present
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None

SCHEMA_NAME = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")

# Read HANA config from environment (Render-friendly). Also allow Streamlit secrets.
def _read_hana_config():
    # Prefer st.secrets if present (Streamlit cloud), otherwise environment variables (Render)
    try:
        if st.secrets and "hana" in st.secrets:
            hana = st.secrets["hana"]
            return {
                "address": hana.get("address"),
                "port": int(hana.get("port", 443)),
                "user": hana.get("user"),
                "password": hana.get("password"),
                "encrypt": hana.get("encrypt", True),
                "sslValidateCertificate": hana.get("sslValidateCertificate", False),
            }
    except Exception:
        # If st.secrets isn't available or access fails, fall back to env
        pass

    return {
        "address": os.environ.get("HANA_ADDRESS"),
        "port": int(os.environ.get("HANA_PORT", 443)),
        "user": os.environ.get("HANA_USER"),
        "password": os.environ.get("HANA_PASSWORD"),
        # env vars come as strings; coerce to bool sensibly
        "encrypt": os.environ.get("HANA_ENCRYPT", "true").lower() in ("1", "true", "yes"),
        "sslValidateCertificate": os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1", "true", "yes"),
    }

hana_config = _read_hana_config()

# Helper: quick TCP test
def tcp_check(host: str, port: int, timeout: float = 6.0):
    try:
        t0 = time.time()
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True, f"TCP ok ({time.time()-t0:.2f}s)"
    except Exception as e:
        return False, str(e)

# CONNECTION MANAGEMENT (no long-lived cached socket)
def _validate_connection(conn):
    """
    Validate a HANA connection by running a lightweight query.
    Return True if valid, False otherwise.
    """
    try:
        if conn is None:
            return False
        # run a tiny validation query
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUMMY")
            _ = cur.fetchall()
        return True
    except Exception:
        return False

def get_connection(retries: int = 4, base_backoff: float = 1.0):
    """
    Return a (conn, cursor) tuple. This function keeps a connection in session_state
    and re-creates it on demand if validation fails. Avoids caching closed sockets.
    """
    if dbapi is None:
        raise RuntimeError("Python package 'hdbcli' is not installed in the environment. Install it (pip install hdbcli) or deploy a proxy/CF app.")

    host = hana_config.get("address")
    port = hana_config.get("port")

    if not host or not port:
        raise RuntimeError("HANA configuration missing. Set HANA_ADDRESS and HANA_PORT environment variables or provide st.secrets['hana'].")

    # if we have a live connection in session_state, validate it and return
    session_conn = st.session_state.get("hana_conn")
    if _validate_connection(session_conn):
        try:
            # Create a cursor on demand for each operation
            cur = session_conn.cursor()
            return session_conn, cur
        except Exception:
            # fall through to create new connection
            pass

    # Pre-check TCP (helpful to fail fast on network issues)
    ok, msg = tcp_check(host, port, timeout=6.0)
    if not ok:
        # We continue to attempt connecting (hdbcli may behave differently), but surface pre-check feedback
        st.warning(f"TCP pre-check to {host}:{port} failed -> {msg}")

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(**hana_config)
            # quick validation
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM DUMMY")
                _ = cur.fetchall()
            # store in session_state for reuse
            st.session_state["hana_conn"] = conn
            cur = conn.cursor()
            return conn, cur
        except Exception as exc:
            last_exc = exc
            backoff = base_backoff * (2 ** (attempt - 1))
            time.sleep(backoff)

    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{tb}")

# Safe helper to close stored connection
def close_session_connection():
    try:
        c = st.session_state.get("hana_conn")
        if c:
            try:
                c.close()
            except Exception:
                pass
            st.session_state.pop("hana_conn", None)
    except Exception:
        pass

# Try to establish connection at startup (fail gracefully with a helpful message)
# We don't keep a global cursor; use get_connection() to obtain a fresh cursor per operation.
try:
    # attempt quick connect to populate session_state
    conn_tmp, cur_tmp = get_connection()
    # close the temporary cursor (we'll create new ones as needed)
    try:
        cur_tmp.close()
    except Exception:
        pass
except Exception as e:
    st.error("🚨 HANA connection failed. See diagnostics below (do NOT commit credentials).")
    st.exception(e)
    # show a minimal diagnostics panel (non-sensitive)
    with st.expander("Connection diagnostics (non-sensitive)"):
        host = hana_config.get("address")
        port = hana_config.get("port")
        st.write("HANA host:", host)
        st.write("HANA port:", port)
        ok, msg = tcp_check(host, port)
        if ok:
            st.success("TCP check: OK (" + msg + ")")
        else:
            st.error("TCP check failed: " + msg)
        st.write("If you see 'Socket closed by peer' or 'Invalid connect reply', the HANA instance may restrict SQL access. Deploy a proxy or Cloud Foundry app in the same BTP subaccount, or ensure the HANA service allows your runner's egress IPs.")
    st.stop()

# DB helper functions (safe, each obtains a fresh cursor)
def fetch_product_names():
    try:
        conn, cursor = get_connection()
        cursor.execute(f'SELECT DISTINCT NAME FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
        rows = cursor.fetchall()
        cursor.close()
        return [r[0] for r in rows] if rows else []
    except Exception as e:
        st.error("Failed to fetch product names from HANA.")
        st.exception(e)
        return []

def get_product_description(name):
    try:
        conn, cursor = get_connection()
        cursor.execute(f'SELECT DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else "No description found."
    except Exception as e:
        st.error("Failed to fetch description.")
        st.exception(e)
        return "Error fetching description."

def insert_product(name, description):
    try:
        conn, cursor = get_connection()
        cursor.execute(f'SELECT MAX(PRODUCT_ID) FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
        row = cursor.fetchone()
        max_id = row[0] if row and row[0] is not None else 0
        new_id = max_id + 1
        cursor.execute(
            f'INSERT INTO "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
            (new_id, name, description, None)
        )
        conn.commit()
        cursor.close()
        return {"status": "ok", "product_id": new_id}
    except Exception as e:
        st.error("Insert failed.")
        st.exception(e)
        return None

def update_product_description(name, new_description):
    try:
        conn, cursor = get_connection()
        cursor.execute(
            f'UPDATE "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" SET DESCRIPTION = ? WHERE NAME = ?',
            (new_description, name)
        )
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        return {"status": "ok", "rows_affected": rows_affected}
    except Exception as e:
        st.error("Update failed.")
        st.exception(e)
        return None

def delete_product(name):
    try:
        conn, cursor = get_connection()
        cursor.execute(f'DELETE FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        return {"status": "ok", "rows_affected": rows_affected}
    except Exception as e:
        st.error("Delete failed.")
        st.exception(e)
        return None

def view_products(limit: int = 200):
    try:
        conn, cursor = get_connection()
        cursor.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID LIMIT {limit}')
        rows = cursor.fetchall()
        cursor.close()
        return rows
    except Exception as e:
        st.error("View products failed.")
        st.exception(e)
        return []

# ----------------- Streamlit UI -----------------
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
            res = insert_product(new_name.strip(), description)
            if res and res.get("status") == "ok":
                st.success(f"Inserted '{new_name}' with PRODUCT_ID={res.get('product_id')}")
            else:
                st.error("Insert did not succeed. Check logs.")

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
                res = update_product_description(choice, new_description.strip())
                if res and res.get("status") == "ok":
                    st.success(f"Updated {res.get('rows_affected', 'N/A')} row(s).")
                else:
                    st.error("Update failed. Check logs.")

elif menu == "Delete Product":
    st.title("🗑️ Delete Product")
    names = fetch_product_names()
    if not names:
        st.warning("No products to delete.")
    else:
        choice = st.selectbox("Product to delete", names)
        if st.button("Delete"):
            res = delete_product(choice)
            if res and res.get("status") == "ok":
                st.success(f"Deleted {res.get('rows_affected', 'N/A')} row(s).")
            else:
                st.error("Delete failed. Check logs.")

# optional close
st.button("Close DB Connection (optional)", on_click=close_session_connection)

# Diagnostics expander (non-sensitive)
with st.expander("Diagnostics / Debug"):
    st.write("Local timestamp:", datetime.now().isoformat())
    st.write("HANA host:", hana_config.get("address"))
    st.write("HANA port:", hana_config.get("port"))
    ok, msg = tcp_check(hana_config.get("address"), hana_config.get("port"))
    if ok:
        st.success("TCP check: " + msg)
    else:
        st.error("TCP check failed: " + msg)
    if dbapi is None:
        st.warning("hdbcli not installed in environment. Install it or deploy a proxy/CF app.")
