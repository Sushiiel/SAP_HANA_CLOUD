import os, time, socket, ssl, traceback
import streamlit as st

# try to import hdbcli
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None

st.set_page_config(page_title="Retail - HANA direct (view only)", layout="wide")

SCHEMA = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")

# ---------- config ----------
def read_hana_config():
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
    return {
        "address": os.environ.get("HANA_ADDRESS") or os.environ.get("HANA_HOST"),
        "port": int(os.environ.get("HANA_PORT", 443)),
        "user": os.environ.get("HANA_USER"),
        "password": os.environ.get("HANA_PASSWORD"),
        "encrypt": os.environ.get("HANA_ENCRYPT", "true").lower() in ("1","true","yes"),
        "sslValidateCertificate": os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1","true","yes"),
    }

hana_cfg = read_hana_config()

# ---------- connection ----------
def tcp_check(host, port, timeout=5):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True, "TCP ok"
    except Exception as e:
        return False, str(e)

def tls_check(host, port, timeout=5):
    try:
        raw = socket.create_connection((host, port), timeout=timeout)
        ctx = ssl.create_default_context()
        ss = ctx.wrap_socket(raw, server_hostname=host)
        _ = ss.cipher()
        ss.close()
        return True, "TLS ok"
    except Exception as e:
        return False, str(e)

def get_hana_connection():
    if dbapi is None:
        raise RuntimeError("hdbcli not installed")
    return dbapi.connect(
        address=hana_cfg.get("address"),
        port=hana_cfg.get("port"),
        user=hana_cfg.get("user"),
        password=hana_cfg.get("password"),
        encrypt=hana_cfg.get("encrypt", True),
        sslValidateCertificate=hana_cfg.get("sslValidateCertificate", False),
    )

# ---------- UI ----------
st.title("Retail - HANA direct (print table)")

with st.expander("Diagnostics (non-sensitive)"):
    st.write("Local timestamp:", time.strftime("%Y-%m-%d %H:%M:%S"))
    st.write("HANA host:", hana_cfg.get("address"))
    st.write("HANA port:", hana_cfg.get("port"))
    ok, msg = tcp_check(hana_cfg.get("address"), hana_cfg.get("port"))
    st.write("TCP:", msg if ok else f"Failed: {msg}")
    ok, msg = tls_check(hana_cfg.get("address"), hana_cfg.get("port"))
    st.write("TLS:", msg if ok else f"Failed: {msg}")
    st.write("hdbcli installed:", bool(dbapi))

st.header("Products table")
try:
    conn = get_hana_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID')
    rows = cur.fetchall()
    cur.close(); conn.close()

    if not rows:
        st.info("No rows found in PRODUCT_EMBEDDINGS.")
    else:
        st.dataframe([{"PRODUCT_ID": r[0], "NAME": r[1], "DESCRIPTION": r[2]} for r in rows])

except Exception as e:
    st.error("❌ Failed to fetch rows from HANA.")
    st.exception(e)
