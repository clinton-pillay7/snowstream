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

st.title("Upload CSV and Send Rows to Redpanda Kafka")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.write("CSV Preview:")
    st.dataframe(df)

    if st.button("Send to Kafka"):
        for _, row in df.iterrows():
            data = row.to_dict()
            producer.produce('kafka_tut', json.dumps(data).encode('utf-8'))
        producer.flush()
        st.success(f" Sent {len(df)} rows to Redpanda Kafka.")
