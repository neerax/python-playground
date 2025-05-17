import streamlit as st
from rag.app import RagApp
from dotenv import load_dotenv
import os


@st.cache_resource()
def get_app():
    load_dotenv(dotenv_path="../../rag/rag/.env")
    return RagApp(os.environ["WEAVIATE_URL"], os.environ["WEAVIATE_API_KEY"])

app = get_app()
wc = app.get_weaviate_client()

result = app.chunks_near_text(st.text_input("Testo da cercare"),st.number_input("k",1),st.number_input("neighbours",0))
result