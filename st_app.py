# st_app.py
# Minimal Streamlit app that connects directly to SAP HANA and shows table rows + simple CRUD.
# WARNING: do NOT commit real credentials to source control. Use platform secrets / env vars.

import os
import time
import socket
import ssl
import traceback
from datetime import datetime

import streamlit as st

# try to import hdbcli; if missing, the app will show an error
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None

st.set_page_config(page_title="Retail - HANA (Direct)", layout="wide", page_icon="🧠")

# ---------- CONFIG: prefer st.secrets['hana'] -> env vars -> FALLBACK (use only for quick local tests) ----------
def _get_hana_config():
    # 1) Streamlit secrets (recommended)
    try:
        if st.secrets and "hana" in st.secrets:
            h = st.secrets["hana"]
            return {
                "address": h.get("address"),
                "port": int(h.get("port", 443)),
                "user": h.get("user"),
                "password": h.get("password"),
                "encrypt": h.get("encrypt", True),
                "sslValidateCertificate": h.get("sslValidateCertificate", False),
            }
    except Exception:
        pass

    # 2) Environment variables
    if os.environ.get("HANA_ADDRESS") or os.environ.get("HANA_HOST"):
        return {
            "address": os.environ.get("HANA_ADDRESS") or os.environ.get("HANA_HOST"),
            "port": int(os.environ.get("HANA_PORT", 443)),
            "user": os.environ.get("HANA_USER"),
            "password": os.environ.get("HANA_PASSWORD"),
            "encrypt": os.environ.get("HANA_ENCRYPT", "true").lower() in ("1","true","yes"),
            "sslValidateCertificate": os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1","true","yes"),
        }

    # 3) FALLBACK (hard-coded) — useful to get app running right away for testing.
    #          REMOVE these defaults before committing to any repo!
    return {
        "address": "6f30991a-1c09-4d97-ad5a-581c884519b0.hana.trial-us10.hanacloud.ondemand.com",
        "port": 443,
        "user": "APP_USER_STREAMLIT",
        "password": "595162sushiielA@",   # <-- Replace / remove before committing
        "encrypt": True,
        "sslValidateCertificate": False,
    }

hana_cfg = _get_hana_config()
SCHEMA_NAME = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")

# ---------- small network checks ----------
def tcp_check(host: str, port: int, timeout: float = 6.0):
    try:
        t0 = time.time()
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True, f"ok ({time.time() - t0:.2f}s)"
    except Exception as e:
        return False, str(e)

def tls_check(host: str, port: int = 443, timeout: float = 6.0):
    try:
        raw = socket.create_connection((host, int(port)), timeout=timeout)
        ctx = ssl.create_default_context()
        ss = ctx.wrap_socket(raw, server_hostname=host)
        _ = ss.cipher()
        ss.close()
        return True, "TLS ok"
    except Exception as e:
        return False, str(e)

# ---------- DB connection (simple, with small retry) ----------
def get_conn(retries: int = 3, backoff_s: float = 1.0):
    if dbapi is None:
        raise RuntimeError("hdbcli is not installed in the environment. Add hdbcli to requirements.")
    host = hana_cfg.get("address")
    port = hana_cfg.get("port")
    if not host or not port:
        raise RuntimeError("HANA host/port not configured.")
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(
                address=host,
                port=port,
                user=hana_cfg.get("user"),
                password=hana_cfg.get("password"),
                encrypt=hana_cfg.get("encrypt", True),
                sslValidateCertificate=hana_cfg.get("sslValidateCertificate", False),
            )
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(backoff_s * attempt)
    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{tb}")

# ---------- Simple CRUD helpers ----------
def fetch_all_products(limit: int = 1000):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID LIMIT {limit}')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        st.error("Failed to fetch products.")
        st.exception(e)
        return []

def insert_product(name: str, description: str):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'SELECT MAX(PRODUCT_ID) FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
        row = cur.fetchone(); max_id = row[0] if row and row[0] is not None else 0
        new_id = max_id + 1
        cur.execute(f'INSERT INTO "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)', (new_id, name, description, None))
        conn.commit(); cur.close(); conn.close()
        return {"status":"ok","product_id":new_id}
    except Exception as e:
        st.error("Insert failed.")
        st.exception(e)
        return {"status":"error","error":str(e)}

def update_product(name: str, new_description: str):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'UPDATE "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" SET DESCRIPTION = ? WHERE NAME = ?', (new_description, name))
        conn.commit(); rows = cur.rowcount; cur.close(); conn.close()
        return {"status":"ok","rows_affected": rows}
    except Exception as e:
        st.error("Update failed.")
        st.exception(e)
        return {"status":"error","error":str(e)}

def delete_product(name: str):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'DELETE FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
        conn.commit(); rows = cur.rowcount; cur.close(); conn.close()
        return {"status":"ok","rows_affected": rows}
    except Exception as e:
        st.error("Delete failed.")
        st.exception(e)
        return {"status":"error","error":str(e)}

# ---------- UI ----------
st.title("Retail - HANA direct (minimal CRUD)")

# show diagnostics and quick checks
with st.expander("Connection diagnostics (non-sensitive)"):
    st.write("Local timestamp:", datetime.utcnow().isoformat())
    st.write("HANA address:", hana_cfg.get("address"))
    st.write("HANA port:", hana_cfg.get("port"))
    ok_tcp, msg_tcp = tcp_check(hana_cfg.get("address"), hana_cfg.get("port"))
    if ok_tcp:
        st.success("TCP: " + msg_tcp)
    else:
        st.error("TCP: " + msg_tcp)
    ok_tls, msg_tls = tls_check(hana_cfg.get("address"), hana_cfg.get("port"))
    if ok_tls:
        st.success("TLS: " + msg_tls)
    else:
        st.warning("TLS: " + msg_tls)
    st.write("hdbcli installed:", dbapi is not None)

# Initial attempt: print table rows
st.header("Products (PRODUCT_EMBEDDINGS)")
rows = fetch_all_products(1000)
if not rows:
    st.info("No rows returned or failed to fetch.")
else:
    # display as dataframe-like table
    st.dataframe([{"PRODUCT_ID": r[0], "NAME": r[1], "DESCRIPTION": r[2]} for r in rows])

st.markdown("---")
# Simple CRUD UI
st.sidebar.title("Actions")
action = st.sidebar.selectbox("Choose", ["Insert","Update","Delete","Refresh Table"])

if action == "Insert":
    st.subheader("Insert new product")
    name = st.text_input("Name", key="ins_name")
    desc = st.text_area("Description", key="ins_desc")
    if st.button("Insert product"):
        if not name.strip():
            st.warning("Enter a product name.")
        else:
            res = insert_product(name.strip(), desc.strip() or "No description")
            if res.get("status") == "ok":
                st.success(f"Inserted {name} (PRODUCT_ID={res.get('product_id')})")
            else:
                st.error("Insert failed. See logs.")

elif action == "Update":
    st.subheader("Update product description")
    product_names = [r[1] for r in rows] if rows else []
    if not product_names:
        st.warning("No products available to update.")
    else:
        sel = st.selectbox("Select product", product_names, key="upd_select")
        new_desc = st.text_area("New description", key="upd_desc")
        if st.button("Update description"):
            if not new_desc.strip():
                st.warning("Enter a new description.")
            else:
                res = update_product(sel, new_desc.strip())
                if res.get("status") == "ok":
                    st.success(f"Updated {res.get('rows_affected',0)} row(s).")
                else:
                    st.error("Update failed.")

elif action == "Delete":
    st.subheader("Delete product")
    product_names = [r[1] for r in rows] if rows else []
    if not product_names:
        st.warning("No products available to delete.")
    else:
        sel = st.selectbox("Select product to delete", product_names, key="del_select")
        if st.button("Delete product"):
            res = delete_product(sel)
            if res.get("status") == "ok":
                st.success(f"Deleted {res.get('rows_affected',0)} row(s).")
            else:
                st.error("Delete failed.")

elif action == "Refresh Table":
    st.experimental_rerun()

st.markdown("---")
st.caption("Security: do NOT commit credentials. Use platform environment variables or Streamlit secrets.")
