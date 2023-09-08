import streamlit as st

import psycopg2 


@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

conn = init_connection()

@st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

rows = run_query("select * from hourly_weather")
for row in rows:
    st.write(f"{row[0]} has a :{row[1]}:")