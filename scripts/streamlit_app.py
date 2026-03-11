import streamlit as st
import pandas as pd

# Title
st.title("Spotify ETL Project Dashboard")

# Upload CSV (example for local testing)
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write("Data Preview:")
    st.dataframe(df)

# Example metric
if uploaded_file:
    st.metric("Number of tracks", len(df))
