from pymongo import MongoClient
from typing import Dict, Any
import pandas as pd

def get_mongo_client(uri: str = "mongodb://mongodb:27017/") -> MongoClient:
    """Connects to the MongoDB instance."""
    return MongoClient(uri)

def insert_dataframe_to_mongo(db_name: str, collection_name: str, data: pd.DataFrame, client: MongoClient) -> None:
    """Inserts a DataFrame into a MongoDB collection."""
    db = client[db_name]
    collection = db[collection_name]
    records = data.to_dict(orient="records")
    collection.insert_many(records)
    print(f"Inserted {len(records)} records into {collection_name}.")