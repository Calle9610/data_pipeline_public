import pytest
from pymongo import MongoClient
import mongomock
import pandas as pd
from mongodb_utils import upsert_dataframe_to_mongo, update_quantity_per_product

# Mock MongoClient to avoid hitting an actual database
@pytest.fixture
def mock_mongo_client():
    # Use mongomock to simulate MongoDB
    return mongomock.MongoClient()

def test_insert_dataframe_to_mongo(mock_mongo_client):
    # Given: A DataFrame to insert
    df = pd.DataFrame([
        {"productId": 1, "name": "Product A", "quantity": 100},
        {"productId": 2, "name": "Product B", "quantity": 150},
    ])

    # When: Insert the DataFrame into the mocked MongoDB collection
    upsert_dataframe_to_mongo('test_db', 'inventory', df, mock_mongo_client, "productId", 2)

    # Then: Verify the collection contains the correct number of documents
    db = mock_mongo_client['test_db']
    collection = db['inventory']
    assert collection.count_documents({}) == 2
    assert collection.find_one({"productId": 1})["name"] == "Product A"
    assert collection.find_one({"productId": 2})["name"] == "Product B"

def test_insert_dataframe_to_mongo_invalid_data(mock_mongo_client):
    # Given: A DataFrame with missing required fields
    df = pd.DataFrame([{"name": "Product A"}])  # Missing 'productId' and 'quantity'

    # When: Insert the invalid DataFrame
    upsert_dataframe_to_mongo('test_db', 'inventory', df, mock_mongo_client, "productId")

    # Then: Ensure no records are inserted due to validation issues (or handle them as appropriate)
    db = mock_mongo_client['test_db']
    collection = db['inventory']
    assert collection.count_documents({}) == 0  # Or assert an error handling mechanism

def test_update_quantity_per_product(mock_mongo_client):
    # Given: An inventory collection with products and orders
    inventory_data = [
        {"productId": 1, "name": "Product A", "quantity": 100, "ordersDetails": [{"quantity": 10}, {"quantity": 20}]},
        {"productId": 2, "name": "Product B", "quantity": 200, "ordersDetails": [{"quantity": 30}]},
    ]
    
    db = mock_mongo_client['test_db']
    collection = db['inventory']
    
    # Insert mock data into inventory collection
    collection.insert_many(inventory_data)

    # When: Call update_quantity_per_product function
    update_quantity_per_product(collection)

    # Then: Verify that the inventory balance has been updated
    updated_item_a = collection.find_one({"productId": 1})
    updated_item_b = collection.find_one({"productId": 2})

    assert updated_item_a["InventoryBalanceAfterOrder"] == 70  # 100 - (10 + 20)
    assert updated_item_b["InventoryBalanceAfterOrder"] == 170  # 200 - 30
