# app.py -- Minimal Flask REST API for SAP HANA CRUD (PRODUCT_EMBEDDINGS) + health
import os
import time
import traceback
from functools import wraps
from typing import Optional, Tuple

from flask import Flask, jsonify, request, abort

# try importing hdbcli
try:
    from hdbcli import dbapi
except Exception as e:
    dbapi = None

app = Flask(__name__)

# ---------- Config ----------
SCHEMA = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")

def _read_hana_cfg():
    # prefer explicit env vars
    return {
        "address": os.environ.get("HANA_ADDRESS") or os.environ.get("HANA_HOST"),
        "port": int(os.environ.get("HANA_PORT", 443)),
        "user": os.environ.get("HANA_USER"),
        "password": os.environ.get("HANA_PASSWORD"),
        "encrypt": os.environ.get("HANA_ENCRYPT", "true").lower() in ("1","true","yes"),
        "sslValidateCertificate": os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1","true","yes"),
    }

# ---------- DB connection helper ----------
def connect_hana(retries: int = 3, backoff: float = 1.0):
    if dbapi is None:
        raise RuntimeError("hdbcli not installed in container. Add it to requirements.txt")
    cfg = _read_hana_cfg()
    host = cfg.get("address")
    port = cfg.get("port")
    if not host or not port:
        raise RuntimeError("HANA config missing. Set HANA_ADDRESS and HANA_PORT env vars.")
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            conn = dbapi.connect(
                address=host,
                port=port,
                user=cfg.get("user"),
                password=cfg.get("password"),
                encrypt=cfg.get("encrypt", True),
                sslValidateCertificate=cfg.get("sslValidateCertificate", False),
                # you can set additional kwargs here if needed
            )
            return conn
        except Exception as e:
            last_exc = e
            time.sleep(backoff * attempt)
    # raise the last exception with traceback
    tb = "".join(traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__))
    raise RuntimeError(f"Unable to connect to HANA after {retries} attempts. Last error: {last_exc}\nTraceback:\n{tb}")

# Helper decorator to catch DB connection errors and return JSON
def hana_route(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as re:
            # likely connection/config issue
            return jsonify({"error": "connection_error", "message": str(re)}), 502
        except Exception as e:
            return jsonify({"error": "internal_error", "message": str(e), "trace": traceback.format_exc()}), 500
    return wrapper

# ---------- Routes ----------
@app.route("/health", methods=["GET"])
def health():
    # quick check: hdbcli present and envs set (non-sensitive)
    has_client = dbapi is not None
    cfg = _read_hana_cfg()
    return jsonify({
        "status": "ok",
        "hdbcli_installed": has_client,
        "hana_address_present": bool(cfg.get("address")),
        "hana_port": cfg.get("port"),
        "schema": SCHEMA
    })

@app.route("/products", methods=["GET"])
@hana_route
def list_products():
    conn = connect_hana()
    cur = conn.cursor()
    # safe query, limit defensively
    cur.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{SCHEMA}"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    products = [{"product_id": r[0], "name": r[1], "description": r[2]} for r in rows]
    return jsonify({"products": products})

@app.route("/product", methods=["POST"])
@hana_route
def insert_product():
    payload = request.get_json(force=True)
    name = payload.get("name")
    description = payload.get("description", "")
    if not name:
        return jsonify({"error": "name_required"}), 400
    conn = connect_hana()
    cur = conn.cursor()
    # compute new id
    cur.execute(f'SELECT MAX(PRODUCT_ID) FROM "{SCHEMA}"."PRODUCT_EMBEDDINGS"')
    r = cur.fetchone()
    max_id = r[0] if r and r[0] is not None else 0
    new_id = max_id + 1
    cur.execute(f'INSERT INTO "{SCHEMA}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
                (new_id, name, description, None))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"status": "ok", "product_id": new_id}), 201

@app.route("/product/<string:name>", methods=["PUT"])
@hana_route
def update_product(name: str):
    payload = request.get_json(force=True)
    description = payload.get("description")
    if description is None:
        return jsonify({"error": "description_required"}), 400
    conn = connect_hana()
    cur = conn.cursor()
    cur.execute(f'UPDATE "{SCHEMA}"."PRODUCT_EMBEDDINGS" SET DESCRIPTION = ? WHERE NAME = ?', (description, name))
    conn.commit()
    rows = cur.rowcount
    cur.close()
    conn.close()
    return jsonify({"status":"ok","rows_affected": rows})

@app.route("/product/<string:name>", methods=["DELETE"])
@hana_route
def delete_product(name: str):
    conn = connect_hana()
    cur = conn.cursor()
    cur.execute(f'DELETE FROM "{SCHEMA}"."PRODUCT_EMBEDDINGS" WHERE NAME = ?', (name,))
    conn.commit()
    rows = cur.rowcount
    cur.close()
    conn.close()
    return jsonify({"status":"ok","rows_affected": rows})

# Health + quick TLS diagnostic endpoint (optional)
@app.route("/tls-test", methods=["GET"])
def tls_test():
    # lightweight diagnostic: does connecting TCP+TLS from this host succeed?
    import socket, ssl
    cfg = _read_hana_cfg()
    host = cfg.get("address")
    port = cfg.get("port")
    out = {"host": host, "port": port}
    try:
        s = socket.create_connection((host, port), timeout=6)
        out["tcp"] = "ok"
        s.close()
    except Exception as e:
        out["tcp_error"] = str(e)
    # try TLS handshake
    try:
        raw = socket.create_connection((host, port), timeout=6)
        ctx = ssl.create_default_context()
        ss = ctx.wrap_socket(raw, server_hostname=host)
        out["tls_cipher"] = ss.cipher()
        ss.close()
    except Exception as e:
        out["tls_error"] = str(e)
    return jsonify(out)

# ---------- startup ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    # when running locally: debug True
    app.run(host="0.0.0.0", port=port, debug=False)
