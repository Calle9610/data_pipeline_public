import os
from ingestion import load_csv
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
    summarize_delivered_orders

)
RAW_DIR = "data/raw/"
DB_NAME = "data_pipeline"
ORDERS_COLLECTION = "orders"
INVENTORY_COLLECTION = "inventory"
COMBINED_COLLECTION = "combined_data"

def main():
    # Load raw data
    orders = load_csv(os.path.join(RAW_DIR, "orders.csv"))
    inventory = load_csv(os.path.join(RAW_DIR, "inventory.csv"))
    
    # TODO: motivera varför jag anser detta rimligt


    print(f"Orders count BEFORE cleaning dublicates: {orders.shape[0]}")
    orders_no_duplicates = orders.drop_duplicates(subset=["orderId"])
    print(f"Orders count AFTER cleaning dublicates: {orders_no_duplicates.shape[0]}")

    print(f"Inventory count BEFORE cleaning dublicates: {inventory.shape[0]}")
    inventory_no_duplicates = inventory.drop_duplicates(subset=["productId"])
    print(f"Inventory count AFTER cleaning dublicates: {inventory_no_duplicates.shape[0]}")
    
    # Connect to MongoDB
    client = get_mongo_client()
    if not client:
        return

    # Insert raw and processed data into MongoDB
    # Prefered to use upsert to insert so this code is reusable, can be run through over and over and avoid dublicates etc. 
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

    # TODO: är denna helt irrelevant även om snygg?
    total_quantity = total_quantity_per_product(inventory_collection)


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