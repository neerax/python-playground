import streamlit as st

from common import app, wc


result = app.chunks_near_text(st.text_input("Testo da cercare"),st.number_input("k",1),st.number_input("neighbours",0))

result