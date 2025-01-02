import pandas as pd
# import os

from astrapy import DataAPIClient
from astrapy.constants import VectorMetric
from astrapy.info import CollectionVectorServiceOptions

# Astra DB connection details
ASTRA_DB_ENDPOINT = (
    "https://e18cea4a-5ccf-4f05-931f-76941a4f569d-us-east-2.apps.astra.datastax.com"
)
ASTRA_DB_TOKEN = "AstraCS:YIFPUWYgeRZuLSirttBcwBDX:f4af626addafb49807fa36018132e113f822942b919077f62391a2ddbcbc541d"

# Hugging Face Serverless configuration
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
API_KEY_NAME = "hack"

# Initialize Astra DB client
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_ENDPOINT)

# Cassandra keyspace and collection
KEYSPACE = "default_keyspace"
COLLECTION = "social_media_data"

if COLLECTION not in db.list_collection_names():
    collection = db.create_collection(
        COLLECTION,
        metric=VectorMetric.COSINE,
        service=CollectionVectorServiceOptions(
            provider="huggingface",
            model_name=MODEL_NAME,
            authentication={
                "providerKey": API_KEY_NAME,
            },
        ),
    )
    print(f"Collection '{COLLECTION}' created successfully!")
else:
    collection = db.get_collection(COLLECTION)
    print(f"Collection '{COLLECTION}' already exists.")

CSV_FILE = "social_media_engagement_data.csv"
data = pd.read_csv(CSV_FILE)

# loading documents into the collection
documents = []
for _, row in data.iterrows():
    document = {
        "post_id": int(row["post_id"]),
        "post_type": row["post_type"],
        "likes": int(row["likes"]),
        "shares": int(row["shares"]),
        "comments": int(row["comments"]),
        "date_posted": row["date_posted"],
        "$vectorize": row["post_type"],
    }
    documents.append(document)

collection.insert_many(documents)

print(
    f"Data from {CSV_FILE} successfully inserted into the {COLLECTION} collection in keyspace {KEYSPACE} with embeddings."
)
