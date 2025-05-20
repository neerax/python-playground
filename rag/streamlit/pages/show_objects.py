import streamlit as st

from common import app, wc

st.set_page_config(layout="wide")

@st.cache_resource()
def get_app():
    load_dotenv(dotenv_path="../../rag/rag/.env")
    return RagApp(os.environ["WEAVIATE_URL"], os.environ["WEAVIATE_API_KEY"])

app = get_app()
wc = app.get_weaviate_client()


objects = app.get_documents()

objects = [o["source"] for o in objects]

for o in objects:
    st.text(o)
