import streamlit as st
import pandas as pd
from confluent_kafka import Producer
import json
import time


# Kafka config
kafka_config = {
    'bootstrap.servers': 'localhost:19092'
}
producer = Producer(kafka_config)




master_headers=['id', 'name', 'age', 'email']
check_header_status = None


st.title("Upload CSV and Send Rows to Redpanda Kafka")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    row_headers = df.columns.tolist()

    if row_headers == master_headers:
        check_header_status = True
        st.write(df)
    else: 
        check_header_status = None
        st.write("Please upload a file with the correct format") 
else:
    st.write("Please upload a file")

if st.button("Submit", disabled=not check_header_status):


    for _, row in df.iterrows():
        data = row.to_dict()
        producer.produce('kafka_tut', json.dumps(data).encode('utf-8'))
    producer.flush()
    st.success(f" Sent {len(df)} rows to Kafka.")



