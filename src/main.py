import os
from ingestion import load_csv
from processing import combine_orders_inventory
from mongodb_utils import get_mongo_client, insert_dataframe_to_mongo

RAW_DIR = "data/raw/"
DB_NAME = "data_pipeline"
ORDERS_COLLECTION = "orders"
INVENTORY_COLLECTION = "inventory"
COMBINED_COLLECTION = "combined_data"

def main():
    # Load raw data
    orders = load_csv(os.path.join(RAW_DIR, "orders.csv"))
    inventory = load_csv(os.path.join(RAW_DIR, "inventory.csv"))
    
    # Process data
    combined_data = combine_orders_inventory(orders, inventory)
    
    # Connect to MongoDB
    client = get_mongo_client()

    # Insert raw and processed data into MongoDB
    insert_dataframe_to_mongo(DB_NAME, ORDERS_COLLECTION, orders, client)
    insert_dataframe_to_mongo(DB_NAME, INVENTORY_COLLECTION, inventory, client)
    insert_dataframe_to_mongo(DB_NAME, COMBINED_COLLECTION, combined_data, client)

    print("Pipeline executed successfully and data saved to MongoDB!")

if __name__ == "__main__":
    main()