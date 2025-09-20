# app.py
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

# Load secrets from Streamlit (ensure you put them in .streamlit/secrets.toml)
# Example secrets.toml:
# [hana]
# address = "host"
# port = 443
# user = "DBADMIN"
# password = "NEW_ROTATED_PASSWORD"
#
# [cohere]
# api_key = "YOUR_COHERE_KEY"

try:
    hana_secrets = st.secrets["hana"]
    cohere_key = st.secrets["cohere"]["api_key"]
except Exception:
    st.error("Missing keys in .streamlit/secrets.toml. Add [hana] and [cohere] entries.")
    st.stop()

hana_config = {
    "address": hana_secrets.get("address"),
    "port": int(hana_secrets.get("port", 443)),
    "user": hana_secrets.get("user"),
    "password": hana_secrets.get("password"),
    # optional flags (use defaults if not present)
    "encrypt": hana_secrets.get("encrypt", True),
    "sslValidateCertificate": hana_secrets.get("sslValidateCertificate", False),
}

# Cohere client
co = cohere.Client(cohere_key)

# --------- Helper: connectivity test ----------
def run_tcp_check(host: str, port: int, timeout: float = 8.0):
    """Return (ok, message)."""
    try:
        t0 = time.time()
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True, f"TCP connect OK to {host}:{port} (latency {time.time()-t0:.2f}s)"
    except Exception as e:
        return False, str(e)

# --------- Cached resources ----------
@st.cache_resource
def load_embed_model():
    return SentenceTransformer("all-MiniLM-L6-v2")

@st.cache_resource
def get_connection(retries: int = 3, backoff_s: float = 1.5):
    """Attempt to connect to HANA with a simple network check then retries.
    Raises informative exceptions if it fails."""
    host = hana_config["address"]
    port = hana_config["port"]

    # Basic network reachability check
    ok, msg = run_tcp_check(host, port, timeout=6.0)
    if not ok:
        raise ConnectionError(f"Network check failed for {host}:{port} -> {msg}")

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            # dbapi.connect expects keyword args matching hana_config fields
            conn = dbapi.connect(**hana_config)
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(backoff_s * attempt)

    # Compose traceback for debugging
    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{tb}")

# ---------- UI: Diagnostics ----------
st.sidebar.header("Diagnostics")
if st.sidebar.button("Run connectivity test to HANA"):
    host = hana_config["address"]
    port = hana_config["port"]
    with st.spinner(f"Testing TCP connection to {host}:{port} ..."):
        ok, msg = run_tcp_check(host, port)
        if ok:
            st.success(f"✅ {msg}")
        else:
            st.error(f"❌ TCP connection failed: {msg}")
            st.write(
                "- If this is deployed on Streamlit Community Cloud, outbound connections to non-HTTPS ports may be blocked.\n"
                "- Check that the host and port are correct and that HANA is reachable from the public internet or via a gateway."
            )
    st.stop()

# ---------- create cached models ----------
embed_model = load_embed_model()

# ---------- attempt to connect to HANA and show clear errors ----------
try:
    conn = get_connection()
    cursor = conn.cursor()
except Exception as e:
    st.error("🚨 Database connection failed. See details below. (Rotate secrets if exposed.)")
    st.exception(e)
    st.stop()

# ---------- App main UI ----------
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
