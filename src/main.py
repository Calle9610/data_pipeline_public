import os
from ingestion import load_csv
from validation import validate_data, remove_dublicates
from mongodb_utils import (
    get_mongo_client,
    upsert_dataframe_to_mongo, 
    total_quantity_per_product,
    combine_orders_to_inventory_with_count,
    update_quantity_per_product,
    get_inventory_with_negative_balance,
    update_order_with_delivery_status,
    get_inventory_with_highest_order,
    summarize_cannot_deliver_orders,
    summarize_delivered_orders,
    store_raw_data_to_mongo

)
RAW_DIR: str = "/app/data/raw/"
PROCESSED_DIR: str = "/app/data/processed/"
DB_NAME: str = "data_pipeline"
RAW_ORDERS_COLLECTION: str = "raw_orders"
RAW_INVENTORY_COLLECTION: str = "raw_inventory"
ORDERS_COLLECTION: str = "orders"
INVENTORY_COLLECTION: str = "inventory"
COMBINED_COLLECTION: str = "combined_data"
CRITICAL_COLUMNS_ORDER: list = ['orderId','productId', 'dateTime', 'quantity']
CRITICAL_COLUMNS_INVENTORY: list = ['productId', 'quantity', 'name', 'quantity']

def main():
    # Load raw data
    orders = load_csv(os.path.join(RAW_DIR, "orders.csv"), PROCESSED_DIR)
    inventory = load_csv(os.path.join(RAW_DIR, "inventory.csv"), PROCESSED_DIR)

    # TODO: motivera varför jag anser detta rimligt
    try:
        validate_data(orders, CRITICAL_COLUMNS_ORDER)
        validate_data(inventory, CRITICAL_COLUMNS_INVENTORY)
        print("Data is valid!")
    except ValueError as e:
        print(f"Validation failed: {e}")

    # Connect to MongoDB
    client = get_mongo_client()
    if not client:
        return
    
    # ingests the two datasets and stores the raw data
    # added last minute after have re-read the instructions
    store_raw_data_to_mongo(DB_NAME, RAW_ORDERS_COLLECTION, orders, client, batch_size=1000)
    store_raw_data_to_mongo(DB_NAME, RAW_INVENTORY_COLLECTION, inventory, client, batch_size=1000)

    # clean dataset from dublicates
    # since i use upsert on my shoosen keys this can see unnecessary
    # but i also wanted to print out to highlight if any dublicates was removed
    orders_no_duplicates = remove_dublicates(orders, 'orderId', ORDERS_COLLECTION)
    inventory_no_duplicates = remove_dublicates(inventory, 'productId', INVENTORY_COLLECTION)

    # Insert raw and processed data into MongoDB
    # I used two collections to keep my changes to the data persisted
    # Prefered to use upsert to insert so this code is reusable, can be run through over and over and avoid creating dublicates etc. 
    upsert_dataframe_to_mongo(DB_NAME, ORDERS_COLLECTION, orders_no_duplicates, client, match_field="orderId", batch_size=1000)
    upsert_dataframe_to_mongo(DB_NAME, INVENTORY_COLLECTION, inventory_no_duplicates, client, match_field="productId", batch_size=1000)

    print("Pipeline executed successfully and data saved to MongoDB!")

   # Connect to the database using the client and specify the database name
    db = client[DB_NAME]

    # Access the "inventory" collection from the database
    inventory_collection = db[INVENTORY_COLLECTION]

    # Access the "orders" collection from the database
    order_collection = db[ORDERS_COLLECTION]

    # Enrich the inventory collection with related order information to simplyfy data access for analytics
    # Use Inventory collection for inventory centric views like manintaining inventory levels
    combine_orders_to_inventory_with_count(inventory_collection)


    print("Inventory updated successfully and data saved to MongoDB!")



    #Calculate and update inventory collection with Inventory balance
    update_quantity_per_product(inventory_collection)

    

    # Enrich the order collection with related inventory information to simplyfy data access for analytics 
    # Use Order collection for order-centric views like delivery and delivery status
    update_order_with_delivery_status(order_collection, inventory_collection)


    # Report queries to display relevant data

    # Display best selling product
    best_selling_product = get_inventory_with_highest_order(inventory_collection)
    print(f"The best selling product is at the moment {best_selling_product['productName']}, with {best_selling_product['totalOrderQuantity']} pieces ordered")

    # Display which products needs to be filled up before orders can be delivered
    inventory_negative_balance = get_inventory_with_negative_balance(inventory_collection)
    print(f" Need to fill up stock on {len(inventory_negative_balance)} products")
    for inv in inventory_negative_balance:
        print(f" Need to fill up stock of: productId: {inv['productId']} and name: {inv['productName']}")

    #
    total_quantity = total_quantity_per_product(inventory_collection)

    # Display which orders can't be delivered and total value od these orders

    not_delivered = summarize_cannot_deliver_orders(order_collection)

    print(f"There are {not_delivered['count']} orders that can't be delivered because of stock shortage.")
    print(f"The total order value of these are {not_delivered['totalAmount']} SEK.")

    # Display which orders has been delivered and total value of these orders
    delivered = summarize_delivered_orders(order_collection)

    print(f"There are {delivered['count']} orders that has been delivered.")
    print(f"The total order value of these are {delivered['totalAmount']} SEK.")

# TODO:
# Se över validering
# Se över typing
# Se över unit testing
# Se över readme
# Se över hur man kör hela applicationen i docker
# Skriv ihop vad jag hade velat göra om jag hade mer tid'
# kommentera koden 


if __name__ == "__main__":
    main()