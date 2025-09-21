# st_app.py (proxy-mode, robust with retries/timeouts)
import os
import requests
import urllib.parse
import streamlit as st
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

st.set_page_config(page_title="Retail Insights (via Proxy)", page_icon="🧠", layout="wide")

# ---------------- Configuration ----------------
def _read_proxy_config():
    try:
        p = st.secrets.get("proxy", None)
        if p and p.get("url") and p.get("api_key"):
            return p["url"].rstrip("/"), p["api_key"]
    except Exception:
        pass
    url = os.environ.get("PROXY_URL") or os.environ.get("PROXY_BASE_URL") or os.environ.get("PROXY_API_URL")
    key = os.environ.get("PROXY_API_KEY") or os.environ.get("PROXY_KEY") or os.environ.get("PROXY_TOKEN")
    if url:
        url = url.rstrip("/")
    return url, key

PROXY_URL, PROXY_KEY = _read_proxy_config()
HEADERS = {"X-API-KEY": PROXY_KEY} if PROXY_KEY else {}
USE_PROXY = bool(PROXY_URL and PROXY_KEY)

# Timeout and retries configurable via env
REQUEST_TIMEOUT = float(os.environ.get("PROXY_TIMEOUT", "30"))  # seconds (default 30)
MAX_RETRIES = int(os.environ.get("PROXY_RETRIES", "3"))
BACKOFF_FACTOR = float(os.environ.get("PROXY_BACKOFF", "0.5"))

# Setup a requests Session with Retry strategy
_session = None
def get_requests_session():
    global _session
    if _session is not None:
        return _session
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 502, 503, 504),
        allowed_methods=frozenset(["GET","POST","PUT","DELETE","HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    _session = session
    return _session

# Helper to show proxy error details
def show_proxy_error(e):
    st.error("Proxy request failed.")
    if isinstance(e, requests.HTTPError):
        try:
            body = e.response.text
        except Exception:
            body = str(e)
        st.code(body)
    else:
        st.exception(e)

# Wrapper helpers
def _raise_for_status_with_body(resp):
    try:
        resp.raise_for_status()
    except requests.HTTPError as he:
        try:
            he.response_text = resp.text
        except Exception:
            he.response_text = None
        raise he

def proxy_get_products():
    if not PROXY_URL:
        raise RuntimeError("Proxy not configured.")
    s = get_requests_session()
    url = f"{PROXY_URL}/products"
    resp = s.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json().get("products", [])

def proxy_insert_product(name: str, description: str):
    s = get_requests_session()
    url = f"{PROXY_URL}/product"
    resp = s.post(url, headers=HEADERS, json={"name": name, "description": description}, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

def proxy_update_product(name: str, description: str):
    s = get_requests_session()
    name_enc = urllib.parse.quote(name, safe="")
    url = f"{PROXY_URL}/product/{name_enc}"
    resp = s.put(url, headers=HEADERS, json={"description": description}, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

def proxy_delete_product(name: str):
    s = get_requests_session()
    name_enc = urllib.parse.quote(name, safe="")
    url = f"{PROXY_URL}/product/{name_enc}"
    resp = s.delete(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    _raise_for_status_with_body(resp)
    return resp.json()

# ---------------- UI ----------------
st.sidebar.title("🔍 Select Action")
menu = st.sidebar.selectbox("", ["Product Insights","Insert Product","View Products","Update Product","Delete Product","Diagnostics"])

def _show_proxy_exception(e):
    st.error("Proxy error — details below.")
    if isinstance(e, requests.HTTPError):
        try:
            st.code(e.response_text or e.response.text)
        except Exception:
            st.write(str(e))
    else:
        st.exception(e)

# Product Insights
if menu == "Product Insights":
    st.title("🔎 Product Insights (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured. Set PROXY_URL and PROXY_API_KEY.")
    else:
        try:
            products = proxy_get_products()
        except Exception as e:
            _show_proxy_exception(e)
            products = []
        names = [p["name"] for p in products]
        if not names:
            st.warning("No products found.")
        else:
            selected = st.selectbox("Select a product", names)
            if st.button("Explain product"):
                desc = next((p.get("description") for p in products if p["name"] == selected), "No description")
                st.info(desc)

# Insert Product
elif menu == "Insert Product":
    st.title("➕ Add New Product (via Proxy)")
    new_name = st.text_input("Product name")
    new_desc = st.text_area("Product description (optional)")
    if st.button("Insert"):
        if not new_name.strip():
            st.warning("Enter product name.")
        else:
            try:
                res = proxy_insert_product(new_name.strip(), new_desc.strip() or "No description")
                st.success(f"Inserted: {res}")
            except Exception as e:
                _show_proxy_exception(e)

# View Products
elif menu == "View Products":
    st.title("📋 All Products (via Proxy)")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            if not products:
                st.warning("No products returned.")
            else:
                st.dataframe([{"PRODUCT_ID": p.get("product_id"), "NAME": p.get("name"), "DESCRIPTION": p.get("description")} for p in products])
        except Exception as e:
            _show_proxy_exception(e)

# Update Product
elif menu == "Update Product":
    st.title("✏️ Update Product")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            names = [p["name"] for p in products]
        except Exception as e:
            _show_proxy_exception(e)
            names = []
        if not names:
            st.warning("No products to update.")
        else:
            choice = st.selectbox("Select product", names)
            new_desc = st.text_area("New description")
            if st.button("Update"):
                if not new_desc.strip():
                    st.warning("Enter new description.")
                else:
                    try:
                        res = proxy_update_product(choice, new_desc.strip())
                        st.success(f"Updated: {res}")
                    except Exception as e:
                        _show_proxy_exception(e)

# Delete Product
elif menu == "Delete Product":
    st.title("🗑️ Delete Product")
    if not USE_PROXY:
        st.warning("Proxy not configured.")
    else:
        try:
            products = proxy_get_products()
            names = [p["name"] for p in products]
        except Exception as e:
            _show_proxy_exception(e)
            names = []
        if not names:
            st.warning("No products to delete.")
        else:
            choice = st.selectbox("Select product", names)
            if st.button("Delete"):
                try:
                    res = proxy_delete_product(choice)
                    st.success(f"Deleted: {res}")
                except Exception as e:
                    _show_proxy_exception(e)

# Diagnostics
elif menu == "Diagnostics":
    st.title("🛠 Diagnostics")
    st.write("Now:", datetime.utcnow().isoformat())
    st.write("Proxy URL:", PROXY_URL or "not configured")
    st.write("Proxy key present:", bool(PROXY_KEY))
    st.write("REQUEST_TIMEOUT (s):", REQUEST_TIMEOUT)
    st.write("MAX_RETRIES:", MAX_RETRIES, "BACKOFF:", BACKOFF_FACTOR)
    if PROXY_URL:
        st.write("Health curl:")
        st.code(f"curl -H 'X-API-KEY: <key>' {PROXY_URL}/health")
        try:
            r = get_requests_session().get(f"{PROXY_URL}/health", headers=HEADERS, timeout=5)
            st.write("HTTP", r.status_code)
            try:
                st.json(r.json())
            except Exception:
                st.text(r.text)
        except Exception as e:
            st.error("Failed to call proxy /health")
            st.exception(e)

st.markdown("---")
st.text("Keep proxy credentials secret. Use platform env vars or Streamlit secrets.")
