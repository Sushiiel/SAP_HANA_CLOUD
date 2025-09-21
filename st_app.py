# st_app.py
import os
import requests
import urllib.parse
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="Retail Insights (via Proxy)", page_icon="🧠")

# ----------------- Configuration (reads secrets first, env fallback) -----------------
def _read_proxy_config():
    # prefer Streamlit secrets (if using Streamlit Cloud)
    try:
        p = st.secrets.get("proxy", None)
        if p and p.get("url") and p.get("api_key"):
            return p["url"].rstrip("/"), p["api_key"]
    except Exception:
        pass

    # fallback to environment variables (Render, other hosts)
    url = os.environ.get("PROXY_URL") or os.environ.get("PROXY_BASE_URL") or os.environ.get("PROXY_API_URL")
    key = os.environ.get("PROXY_API_KEY") or os.environ.get("PROXY_KEY") or os.environ.get("PROXY_TOKEN")
    if url:
        url = url.rstrip("/")
    return url, key

PROXY_URL, PROXY_KEY = _read_proxy_config()
HEADERS = {"X-API-KEY": PROXY_KEY} if PROXY_KEY else {}

USE_PROXY = bool(PROXY_URL and PROXY_KEY)

# ----------------- helper wrappers for proxy endpoints -----------------
REQUEST_TIMEOUT = 15  # seconds

def _raise_for_status_with_body(resp):
    try:
        resp.raise_for_status()
    except requests.HTTPError as he:
        body = ""
        try:
            body = resp.text
        except Exception:
            body = str(he)
        # attach body to exception for easier display
        he.response_text = body
        raise he

def proxy_get_products():
    if not PROXY_URL:
        raise RuntimeError("Proxy not configured")
    url = f"{PROXY_URL}/products"
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json().get("products", [])

def proxy_insert_product(name: str, description: str):
    url = f"{PROXY_URL}/product"
    payload = {"name": name, "description": description}
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

def proxy_update_product(name: str, description: str):
    safe_name = urllib.parse.quote(name, safe="")
    url = f"{PROXY_URL}/product/{safe_name}"
    resp = requests.put(url, headers=HEADERS, json={"description": description}, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

def proxy_delete_product(name: str):
    safe_name = urllib.parse.quote(name, safe="")
    url = f"{PROXY_URL}/product/{safe_name}"
    resp = requests.delete(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

# ----------------- UI helpers -----------------
st.sidebar.title("🔍 Select Action")
menu = st.sidebar.selectbox("", ["Product Insights", "Insert Product", "View Products", "Update Product", "Delete Product", "Diagnostics"])

def _show_proxy_error(e: Exception):
    st.error("Proxy request failed.")
    # If it's an HTTPError from requests, show the response body if available
    if isinstance(e, requests.HTTPError):
        body = getattr(e, "response_text", None)
        if not body and e.response is not None:
            try:
                body = e.response.text
            except Exception:
                body = str(e)
        st.code(body or str(e))
    st.exception(e)

# ----------------- Page: Product Insights -----------------
if menu == "Product Insights":
    st.title("🔎 Product Insights (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured. Please set PROXY_URL and PROXY_API_KEY in env or Streamlit secrets.")
    else:
        try:
            products = proxy_get_products()
        except Exception as e:
            _show_proxy_error(e)
            products = []

        names = [p["name"] for p in products]
        if not names:
            st.warning("No products found (or proxy returned empty).")
        else:
            selected = st.selectbox("Select a product", names)
            if st.button("🧠 Explain Product"):
                # For simplicity we rely on proxy to return description; proxy could also call an LLM on the server side.
                desc = next((p.get("description") for p in products if p["name"] == selected), None)
                if desc:
                    st.info(desc)
                else:
                    st.warning("No description available for this product.")

# ----------------- Page: Insert Product -----------------
elif menu == "Insert Product":
    st.title("➕ Add New Product (via Proxy)")
    new_name = st.text_input("Product name")
    new_desc = st.text_area("Product description (optional)")
    if st.button("Insert"):
        if not new_name.strip():
            st.warning("Enter a product name.")
        else:
            try:
                payload = proxy_insert_product(new_name.strip(), new_desc.strip() or "No description provided.")
                st.success(f"Inserted '{new_name.strip()}' — response: {payload}")
            except Exception as e:
                _show_proxy_error(e)

# ----------------- Page: View Products -----------------
elif menu == "View Products":
    st.title("📋 All Products (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            if not products:
                st.warning("No products found.")
            else:
                st.dataframe([{ "PRODUCT_ID": p.get("product_id"), "NAME": p.get("name"), "DESCRIPTION": p.get("description") } for p in products])
        except Exception as e:
            _show_proxy_error(e)

# ----------------- Page: Update Product -----------------
elif menu == "Update Product":
    st.title("✏️ Update Product (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            names = [p["name"] for p in products]
        except Exception as e:
            _show_proxy_error(e)
            names = []

        if not names:
            st.warning("No products to update.")
        else:
            choice = st.selectbox("Select product to update", names)
            new_description = st.text_area("New description")
            if st.button("Update"):
                if not new_description.strip():
                    st.warning("Enter a new description.")
                else:
                    try:
                        res = proxy_update_product(choice, new_description.strip())
                        st.success(f"Updated: {res}")
                    except Exception as e:
                        _show_proxy_error(e)

# ----------------- Page: Delete Product -----------------
elif menu == "Delete Product":
    st.title("🗑️ Delete Product (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            names = [p["name"] for p in products]
        except Exception as e:
            _show_proxy_error(e)
            names = []

        if not names:
            st.warning("No products to delete.")
        else:
            choice = st.selectbox("Product to delete", names)
            if st.button("Delete"):
                try:
                    res = proxy_delete_product(choice)
                    st.success(f"Deleted: {res}")
                except Exception as e:
                    _show_proxy_error(e)

# ----------------- Diagnostics -----------------
elif menu == "Diagnostics":
    st.title("🛠 Diagnostics")
    st.write("Local time:", datetime.utcnow().isoformat())
    st.write("Proxy configured:", USE_PROXY)
    st.write("Proxy URL:", PROXY_URL or "not configured")
    st.write("Proxy key present:", bool(PROXY_KEY))
    if PROXY_URL:
        st.write("Health endpoint (curl):")
        st.code(f"curl -H 'X-API-KEY: <key>' {PROXY_URL}/health")
        # attempt a quick probe
        try:
            r = requests.get(f"{PROXY_URL}/health", headers=HEADERS, timeout=5)
            # show status and body
            st.write("HTTP", r.status_code)
            try:
                st.json(r.json())
            except Exception:
                st.text(r.text)
        except Exception as e:
            st.error("Failed to call proxy /health")
            st.exception(e)

    st.markdown("---")
    st.markdown("**Note**: Keep proxy credentials (API key) secret. Use platform env vars or Streamlit Secrets.")
