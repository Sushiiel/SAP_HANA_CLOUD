# st_app.py
import os
import socket
import ssl
import time
import traceback
from datetime import datetime

import streamlit as st

# Try import hdbcli; fail gracefully with a helpful message in the UI.
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None

# ---------- CONFIG ----------
SCHEMA_NAME = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")

def _read_hana_config():
    """
    Prefer st.secrets['hana'] (Streamlit Cloud), otherwise environment variables.
    Returns a dict with keys: address, port, user, password, encrypt, sslValidateCertificate
    """
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
        # ignore reading secrets failing (e.g., running locally)
        pass

    return {
        "address": os.environ.get("HANA_ADDRESS"),
        "port": int(os.environ.get("HANA_PORT", 443)),
        "user": os.environ.get("HANA_USER"),
        "password": os.environ.get("HANA_PASSWORD"),
        "encrypt": os.environ.get("HANA_ENCRYPT", "true").lower() in ("1", "true", "yes"),
        "sslValidateCertificate": os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1", "true", "yes"),
    }

# ---------- HELPER CHECKS ----------
def tcp_check(host: str, port: int, timeout: float = 6.0):
    """Simple TCP connect test (returns (ok:bool, message:str))."""
    try:
        t0 = time.time()
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True, f"TCP ok ({time.time()-t0:.2f}s)"
    except Exception as e:
        return False, str(e)

def tls_handshake_check(host: str, port: int = 443, timeout: float = 6.0):
    """Attempt a TLS handshake using ssl.create_default_context()."""
    try:
        raw = socket.create_connection((host, int(port)), timeout=timeout)
        ctx = ssl.create_default_context()
        ssock = ctx.wrap_socket(raw, server_hostname=host)
        # getpeername or cipher to ensure handshake completed
        _ = ssock.cipher()
        ssock.close()
        return True, "TLS handshake OK"
    except Exception as e:
        return False, str(e)

def print_proxy_info():
    p_https = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    p_http = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    st.write("HTTPS_PROXY:", p_https)
    st.write("HTTP_PROXY:", p_http)
    if p_https or p_http:
        st.info("Proxy env detected — ensure the proxy allows outbound traffic to HANA host/port.")

# ---------- INTERACTIVE FALLBACK (session-only) ----------
def _prompt_for_hana_config_if_missing(cfg):
    """
    If address or port missing, show an ephemeral session-only form to allow developer input.
    Values entered are stored only in st.session_state['hana_interactive'] for this session.
    """
    addr = cfg.get("address")
    port = cfg.get("port")
    if not addr or not port:
        st.warning("HANA config missing. Enter connection details below (stored only for this session).")
        with st.form("hana_config_form", clear_on_submit=False):
            host_in = st.text_input("HANA host (address)", value=addr or "")
            port_in = st.text_input("HANA port", value=str(port or 443))
            user_in = st.text_input("HANA user", value=cfg.get("user") or "")
            pwd_in = st.text_input("HANA password (session only)", value="", type="password")
            encrypt_in = st.checkbox("Encrypt (TLS)", value=cfg.get("encrypt", True))
            ssl_validate = st.checkbox("Validate SSL cert", value=cfg.get("sslValidateCertificate", False))
            submitted = st.form_submit_button("Save (session only)")
            if submitted:
                try:
                    shp = {
                        "address": host_in.strip(),
                        "port": int(port_in.strip() or 443),
                        "user": user_in.strip(),
                        "password": pwd_in,
                        "encrypt": encrypt_in,
                        "sslValidateCertificate": ssl_validate,
                    }
                    st.session_state["hana_interactive"] = shp
                    st.success("HANA configuration stored for this session (not persisted).")
                except Exception as e:
                    st.error("Failed to parse values: " + str(e))

        if st.session_state.get("hana_interactive"):
            return st.session_state["hana_interactive"]
        else:
            return cfg
    return cfg

# ---------- CONNECTION MANAGEMENT ----------
def _validate_connection(conn):
    """
    Validate a HANA connection by running a tiny query. Returns True/False.
    """
    try:
        if conn is None:
            return False
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM DUMMY")
            _ = cur.fetchall()
        return True
    except Exception:
        return False

def get_connection(retries: int = 4, base_backoff: float = 1.0):
    """
    Return (conn, cursor). Keeps a validated connection in session_state['hana_conn'].
    Recreates when validation fails. Raises RuntimeError with helpful trace on failure.
    """
    if dbapi is None:
        raise RuntimeError("Python package 'hdbcli' is not installed. Install it (pip install hdbcli) or deploy a proxy/CF app.")

    host = hana_config.get("address")
    port = hana_config.get("port")

    if not host or not port:
        raise RuntimeError("HANA configuration missing. Set HANA_ADDRESS and HANA_PORT environment variables or provide st.secrets['hana'].")

    # If we have a session-stored connection, validate and return
    session_conn = st.session_state.get("hana_conn")
    if _validate_connection(session_conn):
        try:
            cur = session_conn.cursor()
            return session_conn, cur
        except Exception:
            # Fall through to reconnect
            pass

    # Pre-check TCP and TLS (useful diagnostics)
    ok_tcp, tcp_msg = tcp_check(host, port)
    ok_tls, tls_msg = tls_handshake_check(host, port)
    if not ok_tcp:
        st.warning(f"TCP pre-check to {host}:{port} failed -> {tcp_msg}")
    if not ok_tls:
        st.warning(f"TLS handshake pre-check to {host}:{port} failed -> {tls_msg}")

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(**hana_config)
            # Validate with a tiny query
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM DUMMY")
                _ = cur.fetchall()
            st.session_state["hana_conn"] = conn
            cur = conn.cursor()
            return conn, cur
        except Exception as exc:
            last_exc = exc
            backoff = base_backoff * (2 ** (attempt - 1))
            time.sleep(backoff)

    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{tb}")

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

# ---------- STARTUP: load config and possibly prompt ----------
hana_config = _read_hana_config()
hana_config = _prompt_for_hana_config_if_missing(hana_config)

# Quick visible reminder about proxy env (non-sensitive)
proxy_present = bool(os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy"))

# Try to establish a connection (populate session_state) — fail gracefully with detailed diagnostics
try:
    # Only attempt if dbapi present
    if dbapi is None:
        raise RuntimeError("hdbcli not installed in environment.")

    # Quick sanity: host/port present?
    if not hana_config.get("address") or not hana_config.get("port"):
        raise RuntimeError("HANA configuration missing. Set HANA_ADDRESS and HANA_PORT environment variables or provide st.secrets['hana'].")

    # Try connecting (populates st.session_state['hana_conn'])
    conn_tmp, cur_tmp = get_connection()
    try:
        cur_tmp.close()
    except Exception:
        pass

except Exception as e:
    st.error("🚨 HANA connection failed. See diagnostics below (do NOT commit credentials).")
    st.exception(e)
    with st.expander("Connection diagnostics (non-sensitive)"):
        st.write("Local timestamp:", datetime.now().isoformat())
        st.write("HANA host:", hana_config.get("address"))
        st.write("HANA port:", hana_config.get("port"))
        ok_tcp, tcp_msg = tcp_check(hana_config.get("address"), hana_config.get("port"))
        if ok_tcp:
            st.success("TCP check: " + tcp_msg)
        else:
            st.error("TCP check failed: " + tcp_msg)
        ok_tls, tls_msg = tls_handshake_check(hana_config.get("address"), hana_config.get("port"))
        if ok_tls:
            st.success("TLS handshake: " + tls_msg)
        else:
            st.warning("TLS handshake problem: " + tls_msg)
        print_proxy_info()
        if dbapi is None:
            st.warning("hdbcli not installed in environment. Install it (pip install hdbcli) or deploy a proxy/CF app.")
        st.write("If you see 'Socket closed by peer', your runner's egress IP may be blocked by HANA Cloud or HANA requires Cloud Foundry / proxy inside the BTP subaccount.")
    st.stop()

# ---------- DB helper functions ----------
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

# ---------- Streamlit UI ----------
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

# Diagnostics / Debug expander (non-sensitive)
with st.expander("Diagnostics / Debug"):
    st.write("Local timestamp:", datetime.now().isoformat())
    st.write("HANA host:", hana_config.get("address"))
    st.write("HANA port:", hana_config.get("port"))
    ok, msg = tcp_check(hana_config.get("address"), hana_config.get("port"))
    if ok:
        st.success("TCP check: " + msg)
    else:
        st.error("TCP check failed: " + msg)
    ok_tls, tls_msg = tls_handshake_check(hana_config.get("address"), hana_config.get("port"))
    if ok_tls:
        st.success("TLS handshake: " + tls_msg)
    else:
        st.warning("TLS handshake issue: " + tls_msg)
    print_proxy_info()
    if dbapi is None:
        st.warning("hdbcli not installed in environment — install it (pip install hdbcli) or deploy a proxy/CF app.")
