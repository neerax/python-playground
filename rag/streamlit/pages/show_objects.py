import streamlit as st
from common import app, wc

objects = app.get_documents()

objects = [o["source"] for o in objects]

st.set_page_config(layout="wide")

for o in objects:
    st.text(o)
