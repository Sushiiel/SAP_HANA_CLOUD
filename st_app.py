# st_app.py
import streamlit as st
import socket
import traceback
import time
from datetime import datetime
from hdbcli import dbapi

SCHEMA_NAME = "SMART_RETAIL1"

# --- load hana config from Streamlit secrets ---
hana_config = {
    "address": st.secrets["hana"]["address"],
    "port": int(st.secrets["hana"]["port"]),
    "user": st.secrets["hana"]["user"],
    "password": st.secrets["hana"]["password"],
    "encrypt": st.secrets["hana"].get("encrypt", True),
    "sslValidateCertificate": st.secrets["hana"].get("sslValidateCertificate", False)
}

# ----------------- Connection helper -----------------
@st.cache_resource
def get_connection(retries: int = 3, backoff_s: float = 2.0):
    host = hana_config["address"]
    port = hana_config["port"]

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

# Initialize connection
try:
    conn = get_connection()
    cursor = conn.cursor()
except Exception as e:
    st.error("Database connection failed. See details below. (Rotate password if it was exposed.)")
    st.exception(e)
    st.stop()

# ----------------- DB helper functions -----------------
def fetch_product_names():
    cursor.execute(f'SELECT DISTINCT NAME FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
    rows = cursor.fetchall()
    return [r[0] for r in rows] if rows else []

def get_product_description(name):
    cursor.execute(f'SELECT DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
    row = cursor.fetchone()
    return row[0] if row else "No description found."

def insert_product(name, description):
    cursor.execute(f'SELECT MAX(PRODUCT_ID) FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS"')
    row = cursor.fetchone()
    max_id = row[0] if row and row[0] is not None else 0
    new_id = max_id + 1

    cursor.execute(
        f'INSERT INTO "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
        (new_id, name, description, None)
    )
    conn.commit()
    return new_id

def update_product_description(name, new_description):
    cursor.execute(
        f'UPDATE "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" SET DESCRIPTION = ? WHERE NAME = ?',
        (new_description, name)
    )
    conn.commit()
    return cursor.rowcount

def delete_product(name):
    cursor.execute(f'DELETE FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
    conn.commit()
    return cursor.rowcount

def view_products(limit: int = 200):
    cursor.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA_NAME}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID LIMIT {limit}')
    return cursor.fetchall()

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
            try:
                new_id = insert_product(new_name.strip(), description)
                st.success(f"Inserted '{new_name}' with PRODUCT_ID={new_id}")
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
                    rc = update_product_description(choice, new_description.strip())
                    st.success(f"Updated {rc} row(s).")
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
                rc = delete_product(choice)
                st.success(f"Deleted {rc} row(s).")
            except Exception as e:
                st.error(f"Delete failed: {e}")
                st.exception(e)

# optional close
def _close_conn():
    try:
        cursor.close()
        conn.close()
    except Exception:
        pass

st.button("Close DB Connection (optional)", on_click=_close_conn)
