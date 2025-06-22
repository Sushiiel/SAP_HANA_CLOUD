import streamlit as st
import numpy as np
import pickle
import socket
from datetime import datetime
from sentence_transformers import SentenceTransformer
from hdbcli import dbapi
import cohere
import pytz

SCHEMA_NAME="SMART_RETAIL1"
from dotenv import load_dotenv
import os

load_dotenv()

hana_config = {
    'address': os.getenv("HANA_ADDRESS"),
    'port': int(os.getenv("HANA_PORT")),
    'user': os.getenv("HANA_USER"),
    'password': os.getenv("HANA_PASSWORD"),
    'encrypt': True,
    'sslValidateCertificate': False
}
co=cohere.Client("Aa59ZsypfvMKRFOMo7E0O32OAVXNAx6r7okISHYt")

@st.cache_resource
def load_embed_model():
    return SentenceTransformer("all-MiniLM-L6-v2")

embed_model=load_embed_model()

@st.cache_resource
def get_connection():
    return dbapi.connect(**hana_config)

conn=get_connection()
cursor=conn.cursor()

menu=st.sidebar.selectbox("🔍 Select Action",["Product Insights","Insert Product","View Products","Delete Product","Update Product"])

if menu=="Product Insights":
    st.title("🔎 Product Insights from SAP HANA")
    cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
    product_names=[row[0] for row in cursor.fetchall()]
    selected_product=st.selectbox("Select a product",product_names)

    def get_product_description(product_name):
        cursor.execute(f"SELECT DESCRIPTION FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS WHERE NAME=?",(product_name,))
        row=cursor.fetchone()
        return row[0] if row else "No description found."

    def generate_answer(product_desc):
        prompt=f"Explain the following product for a customer: {product_desc}"
        response=co.generate(model="command-r-plus",prompt=prompt,max_tokens=100,temperature=0.5)
        return response.generations[0].text.strip()

    def log_conversation(question,answer):
        timestamp=datetime.utcnow().strftime("%Y%m%d%H%M%S")
        cursor.execute(f"INSERT INTO {SCHEMA_NAME}.CHAT_LOG (ID,QUESTION,ANSWER,TIMESTAMP) VALUES (?,?,?,?)",(int(timestamp),question,answer,datetime.utcnow()))
        conn.commit()

    if selected_product:
        desc=get_product_description(selected_product)
        if st.button("🧠 Explain Product"):
            with st.spinner("Generating explanation..."):
                try:
                    response=generate_answer(desc)
                    st.success(response)
                    log_conversation(selected_product,response)
                except Exception as e:
                    st.error(f"❌ Error: {e}")

elif menu=="Insert Product":
    st.title("➕ Add New Product Using AI")
    new_name=st.text_input("Enter product name")
    if st.button("✨ Generate & Insert Product"):
        if not new_name.strip():
            st.warning("Please enter a product name.")
        else:
            try:
                prompt=f"Write a 10-word product description for: {new_name}"
                co_response=co.generate(model="command-r-plus",prompt=prompt,max_tokens=50,temperature=0.7)
                description=co_response.generations[0].text.strip()
                vector=embed_model.encode(description)
                vector_blob=pickle.dumps(vector)
                cursor.execute(f"SELECT MAX(PRODUCT_ID) FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
                max_id=cursor.fetchone()[0] or 0
                new_id=max_id+1
                local_time=datetime.now(pytz.timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
                ip=socket.gethostbyname(socket.gethostname())
                cursor.execute(f"INSERT INTO {SCHEMA_NAME}.PRODUCT_EMBEDDINGS (PRODUCT_ID,NAME,DESCRIPTION,VECTOR) VALUES (?,?,?,?)",(new_id,new_name,description,vector_blob))
                conn.commit()
                st.success(f"✅ '{new_name}' inserted with description: {description}")
                st.info(f"🕓 Local Time: {local_time} | 🌐 IP: {ip}")
            except Exception as e:
                st.error(f"❌ Insertion failed: {e}")

elif menu=="View Products":
    st.title("📋 All Products")
    cursor.execute(f"SELECT PRODUCT_ID,NAME,DESCRIPTION FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
    rows=cursor.fetchall()
    if rows:
        st.dataframe(rows,hide_index=True)
    else:
        st.warning("No products found.")

elif menu=="Delete Product":
    st.title("🗑️ Delete Product")
    cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
    names=[row[0] for row in cursor.fetchall()]
    del_name=st.selectbox("Select a product to delete",names)
    if st.button("❌ Delete Selected Product"):
        try:
            cursor.execute(f"DELETE FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS WHERE NAME=?",(del_name,))
            conn.commit()
            st.success(f"✅ '{del_name}' deleted successfully.")
        except Exception as e:
            st.error(f"❌ Delete failed: {e}")

elif menu=="Update Product":
    st.title("✏️ Update Product Description")
    cursor.execute(f"SELECT DISTINCT NAME FROM {SCHEMA_NAME}.PRODUCT_EMBEDDINGS")
    names=[row[0] for row in cursor.fetchall()]
    upd_name=st.selectbox("Select product to update",names)
    new_desc=st.text_input("Enter new description")
    if st.button("🔄 Update"):
        try:
            new_vec=embed_model.encode(new_desc)
            vec_blob=pickle.dumps(new_vec)
            cursor.execute(f"UPDATE {SCHEMA_NAME}.PRODUCT_EMBEDDINGS SET DESCRIPTION=?,VECTOR=? WHERE NAME=?",(new_desc,vec_blob,upd_name))
            conn.commit()
            st.success("✅ Updated successfully.")
        except Exception as e:
            st.error(f"❌ Update failed: {e}")
