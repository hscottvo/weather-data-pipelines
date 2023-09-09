import streamlit as st

import psycopg2


@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])




@st.cache_data(ttl=600)
def run_query(query):
    conn = init_connection()
    if conn == None:
        exit(1)
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


rows = run_query("select * from hourly_weather;")
for row in rows:
    st.write(f"{row[0]} has a :{row[1]}:")
