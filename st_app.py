# app.py (updated - advanced port/protocol detection + automatic port switch)
import streamlit as st
import socket
import time
import pickle
import traceback
from datetime import datetime
import pytz

from sentence_transformers import SentenceTransformer
from hdbcli import dbapi
import cohere

# -------- CONFIG / CONSTANTS ----------
SCHEMA_NAME = "SMART_RETAIL1"

# Load secrets
try:
    hana_secrets = st.secrets["hana"]
    cohere_key = st.secrets["cohere"]["api_key"]
except Exception:
    st.error("Missing keys in .streamlit/secrets.toml. Add [hana] and [cohere] entries.")
    st.stop()

# initial hana_config (port may be replaced after diagnostics)
hana_config = {
    "address": hana_secrets.get("address"),
    "port": int(hana_secrets.get("port", 443)),
    "user": hana_secrets.get("user"),
    "password": hana_secrets.get("password"),
    "encrypt": hana_secrets.get("encrypt", True),
    "sslValidateCertificate": hana_secrets.get("sslValidateCertificate", False),
}

co = cohere.Client(cohere_key)

# ---------- utilities ----------
def tcp_probe(host: str, port: int, timeout: float = 6.0):
    """
    Try TCP connect; return tuple (ok:bool, latency:float|None, banner_preview:str).
    """
    try:
        t0 = time.time()
        s = socket.create_connection((host, port), timeout=timeout)
        latency = time.time() - t0
        s.settimeout(1.0)
        try:
            data = s.recv(512)
        except Exception:
            data = b""
        s.close()
        preview = data.decode("latin-1", errors="replace")[:300]
        return True, latency, preview
    except Exception as e:
        return False, None, str(e)

def try_hdbcli_connect(host, port, user, password, encrypt=True, sslValidateCertificate=False, timeout_seconds=8):
    """Attempt dbapi.connect for a short time, close immediately if successful."""
    cfg = {
        "address": host,
        "port": port,
        "user": user,
        "password": password,
        "encrypt": encrypt,
        "sslValidateCertificate": sslValidateCertificate,
    }
    try:
        conn = dbapi.connect(**cfg)
        conn.close()
        return True, None
    except Exception as e:
        return False, e

# ---------- advanced diagnostics UI ----------
st.sidebar.header("Diagnostics")
if st.sidebar.button("Run advanced connectivity & protocol check"):
    host = hana_config["address"]
    # candidate ports: configured one first, then common SAP HANA SQL ports
    candidate_ports = [hana_config["port"], 30015, 39015, 30013]
    seen = set()
    results = []
    for p in candidate_ports:
        if p in seen:
            continue
        seen.add(p)
        ok, latency, preview = tcp_probe(host, p)
        if ok:
            st.success(f"TCP open to {host}:{p} (latency {latency:.2f}s)")
            if preview:
                # quick heuristic for HTTP
                if preview.startswith("HTTP/") or preview.startswith("GET") or "Server:" in preview[:50] or "HTTP" in preview[:20]:
                    st.info(f"Port {p} appears to speak HTTP/HTTPS (preview: {preview[:160]!r})")
                else:
                    st.info(f"Port {p} returned bytes (preview: {preview[:160]!r}) — could be DB listener")
            else:
                st.info(f"Port {p} accepted TCP but returned no immediate banner.")
            st.write(f"Trying hdbcli.connect() on {host}:{p} ...")
            success, err = try_hdbcli_connect(
                host, p,
                hana_secrets.get("user"),
                hana_secrets.get("password"),
                hana_secrets.get("encrypt", True),
                hana_secrets.get("sslValidateCertificate", False)
            )
            if success:
                st.success(f"hdbcli.connect() SUCCESS on port {p}! — this is the correct SQL port.")
            else:
                st.error(f"hdbcli.connect() FAILED on port {p}: {err}")
                st.exception(err)
        else:
            st.error(f"TCP connect failed to {host}:{p} -> {preview}")
    st.stop()

# ---------- cached resources ----------
@st.cache_resource
def load_embed_model():
    return SentenceTransformer("all-MiniLM-L6-v2")

@st.cache_resource
def get_connection(retries: int = 3, backoff_s: float = 1.5):
    """
    Before trying the final dbapi.connect, attempt an intelligent port detection:
    - probe candidate ports and if hdbcli.connect() succeeds on any, use that port.
    - otherwise fall back to hana_config['port'] (which may be 443) and attempt connect (with retries).
    """
    host = hana_config["address"]
    user = hana_config["user"]
    password = hana_config["password"]
    encrypt = hana_config.get("encrypt", True)
    sslValidateCertificate = hana_config.get("sslValidateCertificate", False)

    # Candidate ports to try (configured first, then common HANA SQL ports)
    candidate_ports = [hana_config["port"], 30015, 39015, 30013]
    tried_ports = set()
    working_port = None

    for p in candidate_ports:
        if p in tried_ports:
            continue
        tried_ports.add(p)
        ok, latency, preview = tcp_probe(host, p)
        if not ok:
            continue
        # If TCP is open, try hdbcli.connect quickly
        success, err = try_hdbcli_connect(host, p, user, password, encrypt, sslValidateCertificate)
        if success:
            working_port = p
            hana_config["port"] = p  # update config to use working port
            break

    # If we found a working port, do final connect and return
    if working_port:
        # attempt final connect with retries (should succeed)
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                conn = dbapi.connect(**hana_config)
                return conn
            except Exception as e:
                last_exc = e
                time.sleep(backoff_s * attempt)
        tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
        raise RuntimeError(f"Found candidate SQL port {working_port} but final connect failed: {last_exc}\nTraceback:\n{tb}")

    # No working port discovered — attempt to connect to configured port (may be 443 HTTP) with retries
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(**hana_config)
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(backoff_s * attempt)

    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    # Provide guidance in the exception text so UI shows it
    raise RuntimeError(
        "Unable to connect to HANA. No candidate SQL port accepted hdbcli, and direct connect to configured port failed.\n"
        f"Last error: {last_exc}\nTraceback:\n{tb}\n\n"
        "If port 443 responded but hdbcli failed with 'Invalid connect reply', that means 443 is an HTTP endpoint (not HANA SQL).\n"
        "Options: (1) find the SQL port from your HANA service credentials (sql_port) and add it to .streamlit/secrets.toml, or\n"
        "(2) deploy a small proxy (Flask/FastAPI) on a host that can access the SQL port and call that over HTTPS from Streamlit Cloud."
    )

# ---------- run model load and attempt connection ----------
embed_model = load_embed_model()

try:
    conn = get_connection()
    cursor = conn.cursor()
except Exception as e:
    st.error("🚨 Database connection failed. See details below. (Rotate secrets if exposed.)")
    st.exception(e)
    st.stop()

# ---------- App main UI (same as before) ----------
menu = st.sidebar.selectbox("🔍 Select Action",
                            ["Product Insights", "Insert Product", "View Products", "Delete Product", "Update Product"])

if menu == "Product Insights":
    st.title("🔎 Product Insights from SAP HANA")
    try:
        cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
        product_names = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        st.error("Failed to fetch product list from the DB.")
        st.exception(e)
        st.stop()

    selected_product = st.selectbox("Select a product", product_names or ["(no products found)"])

    def get_product_description(product_name):
        cursor.execute(f"SELECT DESCRIPTION FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS WHERE NAME=?", (product_name,))
        row = cursor.fetchone()
        return row[0] if row else "No description found."

    def generate_answer(product_desc):
        prompt = f"Explain the following product for a customer: {product_desc}"
        response = co.generate(model="command-r-plus", prompt=prompt, max_tokens=100, temperature=0.5)
        return response.generations[0].text.strip()

    def log_conversation(question, answer):
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        cursor.execute(
            f"INSERT INTO {SCHEMA_NAME}.CHAT_LOG (ID,QUESTION,ANSWER,TIMESTAMP) VALUES (?,?,?,?)",
            (int(timestamp), question, answer, datetime.utcnow())
        )
        conn.commit()

    if selected_product and selected_product != "(no products found)":
        desc = get_product_description(selected_product)
        if st.button("🧠 Explain Product"):
            with st.spinner("Generating explanation..."):
                try:
                    response = generate_answer(desc)
                    st.success(response)
                    log_conversation(selected_product, response)
                except Exception as e:
                    st.error(f"❌ Error while generating explanation: {e}")
                    st.exception(e)

elif menu == "Insert Product":
    st.title("➕ Add New Product Using AI")
    new_name = st.text_input("Enter product name")
    if st.button("✨ Generate & Insert Product"):
        if not new_name.strip():
            st.warning("Please enter a product name.")
        else:
            try:
                prompt = f"Write a 10-word product description for: {new_name}"
                co_response = co.generate(model="command-r-plus", prompt=prompt, max_tokens=50, temperature=0.7)
                description = co_response.generations[0].text.strip()
                vector = embed_model.encode(description)
                vector_blob = pickle.dumps(vector)

                cursor.execute(f"SELECT MAX(PRODUCT_ID) FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
                row = cursor.fetchone()
                max_id = row[0] if row and row[0] is not None else 0
                new_id = max_id + 1

                local_time = datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
                try:
                    ip = socket.gethostbyname(socket.gethostname())
                except Exception:
                    ip = "unknown"

                cursor.execute(
                    f"INSERT INTO {SCHEMA_NAME}.PRODUCT_EMBEDDINGS (PRODUCT_ID,NAME,DESCRIPTION,VECTOR) VALUES (?,?,?,?)",
                    (new_id, new_name, description, vector_blob)
                )
                conn.commit()
                st.success(f"✅ '{new_name}' inserted with description: {description}")
                st.info(f"🕓 Local Time: {local_time} | 🌐 IP: {ip}")
            except Exception as e:
                st.error(f"❌ Insertion failed: {e}")
                st.exception(e)

elif menu == "View Products":
    st.title("📋 All Products")
    try:
        cursor.execute(f"SELECT PRODUCT_ID,NAME,DESCRIPTION FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
        rows = cursor.fetchall()
        if rows:
            st.dataframe(rows, hide_index=True)
        else:
            st.warning("No products found.")
    except Exception as e:
        st.error("Failed to fetch products.")
        st.exception(e)

elif menu == "Delete Product":
    st.title("🗑️ Delete Product")
    try:
        cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
        names = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        st.error("Failed to fetch product names.")
        st.exception(e)
        st.stop()

    if not names:
        st.warning("No products to delete.")
    else:
        del_name = st.selectbox("Select a product to delete", names)
        if st.button("❌ Delete Selected Product"):
            try:
                cursor.execute(f"DELETE FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS WHERE NAME=?", (del_name,))
                conn.commit()
                st.success(f"✅ '{del_name}' deleted successfully.")
            except Exception as e:
                st.error(f"❌ Delete failed: {e}")
                st.exception(e)

elif menu == "Update Product":
    st.title("✏️ Update Product Description")
    try:
        cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
        names = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        st.error("Failed to fetch product names.")
        st.exception(e)
        st.stop()

    if not names:
        st.warning("No products to update.")
    else:
        upd_name = st.selectbox("Select product to update", names)
        new_desc = st.text_input("Enter new description")
        if st.button("🔄 Update"):
            if not new_desc.strip():
                st.warning("Please enter a new description.")
            else:
                try:
                    new_vec = embed_model.encode(new_desc)
                    vec_blob = pickle.dumps(new_vec)
                    cursor.execute(
                        f"UPDATE {SCHEMA_NAME}.PRODUCT_EMBEDDINGS SET DESCRIPTION=?,VECTOR=? WHERE NAME=?",
                        (new_desc, vec_blob, upd_name)
                    )
                    conn.commit()
                    st.success("✅ Updated successfully.")
                except Exception as e:
                    st.error(f"❌ Update failed: {e}")
                    st.exception(e)
