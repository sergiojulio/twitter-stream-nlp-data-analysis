 
import streamlit as st

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Perform query.
df = conn.query('SELECT * FROM stream;', ttl="10m") # ttl for caching

# Print results.
for row in df.itertuples():
    st.write(f"{row.text} has a :{row.polarity}:")