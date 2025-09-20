import streamlit as st
import socket
import traceback
from hdbcli import dbapi
import time

SCHEMA_NAME = "SMART_RETAIL1"

# Load config from secrets (safer than hardcoding)
hana_config = {
    "address": st.secrets["hana"]["address"],
    "port": int(st.secrets["hana"]["port"]),
    "user": st.secrets["hana"]["user"],
    "password": st.secrets["hana"]["password"],
    "encrypt": st.secrets["hana"].get("encrypt", True),
    "sslValidateCertificate": st.secrets["hana"].get("sslValidateCertificate", False)
}

@st.cache_resource
def get_connection(retries: int = 3, backoff_s: float = 2.0):
    host = hana_config["address"]
    port = hana_config["port"]

    # Quick network check first
    try:
        with socket.create_connection((host, port), timeout=8):
            pass
    except Exception as e:
        # Surface a clear error in the UI/logs
        raise ConnectionError(f"Network check failed when attempting to reach {host}:{port} — {e}")

    # Try connecting with retries, logging real exception
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(**hana_config)
            return conn
        except Exception as e:
            last_exc = e
            # small backoff between retries
            time.sleep(backoff_s * attempt)
    # After retries, raise an informative error including original traceback for logs
    tb = traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__)
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{''.join(tb)}")

# Use connection in the app; if it fails, show the exception for debugging
try:
    conn = get_connection()
    cursor = conn.cursor()
except Exception as e:
    st.error("Database connection failed. See more details below (rotate password if it was exposed).")
    # Show the friendly message, and print full traceback to logs/UI for debugging
    st.exception(e)
    st.stop()
