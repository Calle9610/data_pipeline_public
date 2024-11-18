from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.collection import Collection
from typing import Dict, Any, List
import pandas as pd
import logging

def get_mongo_client(uri: str = "mongodb://mongodb:27017/") -> MongoClient:
    """Connects to the MongoDB instance."""
    # for local development: "mongodb://localhost:27017/"
    try:
        client = MongoClient(uri)
        print(f"Connect to MongoDB")
        return client
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

def store_raw_data_to_mongo(db_name: str, collection_name: str, df: pd.DataFrame, client: MongoClient, batch_size: int):
    """
    Store raw data in MongoDB collection using bulk_write for efficiency.
    """
    db = client[db_name]
    collection = db[collection_name]
    
    # Convert DataFrame rows to a list of InsertOne operations
    operations = [InsertOne(record) for record in df.to_dict('records')]
    
    # Batch and execute bulk writes
    for i in range(0, len(operations), batch_size):
        batch = operations[i:i + batch_size]
        collection.bulk_write(batch)

def upsert_dataframe_to_mongo(db_name: str, collection_name: str, df: pd.DataFrame, client: MongoClient, match_field: str, batch_size: int = 1000) -> None:
    """
    Upsert a DataFrame into a MongoDB collection in batches.

    Args:
        db_name (str): The name of the database.
        collection_name (str): The name of the collection.
        df (pd.DataFrame): The DataFrame to upsert.
        client (MongoClient): The MongoDB client.
        match_field (str): The field to match for upsert.
        batch_size (int): The size of each batch for upserts.
    """
    # Validate input
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    
    if match_field not in df.columns:
        raise ValueError(f"match_field '{match_field}' is not a valid column in the DataFrame.")
    
    if df.empty:
        logging.warning("The provided DataFrame is empty. No data to upsert.")
        return
    
    try:
        db = client[db_name]
        collection = db[collection_name]

        # Convert DataFrame to a list of dictionaries
        records = df.to_dict("records")

        # Prepare operations for bulk_write
        operations = []
        for record in records:
            # Check if the match_field exists in the record
            if match_field not in record:
                logging.warning(f"Record {record} does not contain the match_field '{match_field}', skipping this record.")
                continue  # Skip the record if match_field is missing

            query = {match_field: record[match_field]}
            update = {"$set": record}
            operations.append(UpdateOne(query, update, upsert=True))

            # Perform bulk write when the batch size is reached
            if len(operations) >= batch_size:
                collection.bulk_write(operations)
                operations = []  # Clear the operations list for the next batch

        # If there are any remaining operations after the loop, execute them
        if operations:
            collection.bulk_write(operations)

    except Exception as e:
        logging.error(f"An error occurred during upsert: {e}")
        raise  # Re-raise the exception after logging it

def combine_orders_to_inventory_with_count(inventory: Collection) -> None:
    pipeline = [
        {
            "$lookup": {
                "from": "orders",  # Join the orders collection
                "localField": "productId",  # Match inventory's productId
                "foreignField": "productId",  # Match orders' productId
                "as": "ordersDetails"  # Alias for joined documents
            }
        },
        {
            "$addFields": {
                "ordersDetailsCount": { "$size": "$ordersDetails" },  # Count the connected orders
            }
        }
    ]
    # Execute the pipeline and update the collection
    inventory_with_orders = list(inventory.aggregate(pipeline))
    for item in inventory_with_orders:
        inventory.update_one(
            {"_id": item["_id"]},
            {"$set": {"ordersDetails": item["ordersDetails"], "ordersDetailsCount": item["ordersDetailsCount"]}}
        )

def total_quantity_per_product(inventory: Collection) -> list:
    pipeline = [
        {
            "$unwind": "$ordersDetails"  # Unwind the orders details to flatten the structure
        },
        {
            "$group": {
                "_id": "$productId",  # Group by productId
                "Productname": {"$first": "$name"},
                "InventoryQuantity": {"$first": "$quantity"},
                "totalQuantityOrdered": {"$sum": "$ordersDetails.quantity"}  # Sum the quantity from orders
            }
        },
        {
            "$sort": {"totalQuantityOrdered": -1}  # Sort by totalQuantityOrdered in descending order
        }
    ]
    
    result = list(inventory.aggregate(pipeline))
    return result

def update_quantity_per_product(inventory: Collection) -> None:
    pipeline = [
        {
            "$unwind": "$ordersDetails"  # Unwind the orders (breaks them to individual documents) details to flatten the structure
        },
        {
            "$group": {
                "_id": "$productId",  # Group by productId
                "Productname": {"$first": "$name"},  # Get the first product name
                "InventoryQuantity": {"$first": "$quantity"},  # Get the first inventory quantity
                "totalQuantityOrdered": {"$sum": "$ordersDetails.quantity"}  # Sum the quantity from orders
            }
        },
        {
            "$addFields": {
                "InventoryBalanceAfterOrder": {
                    "$subtract": ["$InventoryQuantity", "$totalQuantityOrdered"]  # Calculate inventory balance
                }
            }
        }
    ]

    # Perform aggregation
    inventory_with_orders = list(inventory.aggregate(pipeline))

        # Prepare bulk operations
    bulk_operations = []
    for item in inventory_with_orders:
        # Build the update operation for each document
        bulk_operations.append(
            UpdateOne(
                {"productId": item["_id"]},  # Match inventory by productId
                {
                    "$set": {
                        "totalQuantityOrdered": item["totalQuantityOrdered"],
                        "InventoryBalanceAfterOrder": item["InventoryBalanceAfterOrder"]
                    }
                }
            )
        )

    # Perform the bulk write operation
    if bulk_operations:
        result = inventory.bulk_write(bulk_operations)
        print(f"Bulk update complete: Matched {result.matched_count} documents, "
              f"Modified {result.modified_count} documents.")


def get_inventory_with_negative_balance(inventory: Collection) -> list:
    # MongoDB query to find all inventory with a negative InventoryBalanceAfterOrder
    query = { "InventoryBalanceAfterOrder": { "$lt": 0 } }

    # MongoDB projection to return only the productId and productName fields
    projection = { "productId": 1, "name": 1 }

    # Fetch documents matching the query and apply the projection
    negative_balance_inventory = list(inventory.find(query, projection))

    # Return a list of dictionaries with only productId and productName
    return [{"productId": item["productId"], "productName": item["name"]} for item in negative_balance_inventory]


def update_order_with_delivery_status(orders: Collection, inventory: Collection):
    # Fetch inventory data as a dictionary with productId as the key
    inventory_data = [
        {
            "productId": item["productId"],
            "InventoryBalanceAfterOrder": item.get("InventoryBalanceAfterOrder", None),  # Default to None if attribute doesn't exist
            "quantity": item["quantity"]
        }
        for item in inventory.find()
        if item.get("InventoryBalanceAfterOrder", None) and item["InventoryBalanceAfterOrder"] < 0  # Only include if the attribute exists and is negative
    ]
    
    # Fetch all orders from the orders collection
    # all_orders = list(orders.find())
    updated_orders = []

    for data in inventory_data:
        quantity = data['quantity']
        inv_orders = orders.find({"productId": data['productId']})
        # all_orders.get(data['productId'])
        if inv_orders:
            # Sort the orders by the 'dateTime' field (oldest first)
            sorted_inv_orders = sorted(inv_orders, key=lambda order: order['dateTime'])

              # Loop through the sorted orders
            for order in sorted_inv_orders:
                quantity = quantity - order['quantity']
                if quantity < 0:
                    delivery_status = "Cannot Deliver"
                    orders.update_one(
                        {"_id": order["_id"]},  # Match by order ID
                        {"$set": {"deliveryStatus": delivery_status}}
                    )
                    updated_orders.append(order['_id'])

                    print(f"Order with Id: {order['_id']} can't be delivered, database is updated")
    
    orders_to_update = orders.find({"_id": {"$nin": updated_orders}})  # Exclude orders in updated_orders
            
    for order in orders_to_update:
        # Update orders that were not marked as "Cannot Deliver"
        orders.update_one(
            {"_id": order["_id"]},  # Match by order ID
            {"$set": {"deliveryStatus": "Delivered"}}
        )
        print(f"Order with Id: {order['_id']} has been delivered, database is updated")

def get_inventory_with_highest_order(inventory: Collection) -> dict:
    """
    Finds the inventory item with the highest total order quantity.
    
    Args:
        inventory (Collection): The MongoDB inventory collection.
        
    Returns:
        dict: The inventory item with the highest total order quantity.
    """
    # Aggregation pipeline to summarize ordersDetails.quantity for each inventory
    pipeline = [
        # Unwind ordersDetails to access individual quantities
        {"$unwind": "$ordersDetails"},
        
        # Group by productId and calculate the total order quantity
        {
            "$group": {
                "_id": "$productId",  # Group by productId
                "productName": {"$first": "$name"},  # Include productName
                "totalOrderQuantity": {"$sum": "$ordersDetails.quantity"},  # Summarize quantities
            }
        },
        
        # Sort by totalOrderQuantity in descending order
        {"$sort": {"totalOrderQuantity": -1}},
        
        # Limit to the top result
        {"$limit": 1},
    ]
    
    # Run the aggregation pipeline
    result = list(inventory.aggregate(pipeline))
    
    # Return the top result (or None if no results)
    return result[0] if result else None

def summarize_cannot_deliver_orders(orders_collection: Collection) -> dict:
    """
    Counts the number of orders with deliveryStatus = "Cannot Deliver" and sums the amount for these orders.
    
    Args:
        orders_collection (Collection): The MongoDB orders collection.
        
    Returns:
        dict: A dictionary containing the count of orders and the total amount.
    """
    # MongoDB query to match orders with deliveryStatus = "Cannot Deliver"
    query = {"deliveryStatus": "Cannot Deliver"}
    
    # Count the number of matching documents
    count = orders_collection.count_documents(query)
    
    # Aggregate to calculate the total amount
    pipeline = [
        {"$match": query},  # Filter documents matching the query
        {"$group": {"_id": None, "totalAmount": {"$sum": "$amount"}}}  # Sum the amount field
    ]
    
    # Execute the aggregation pipeline
    result = list(orders_collection.aggregate(pipeline))
    total_amount = result[0]["totalAmount"] if result else 0
    
    return {
        "count": count,
        "totalAmount": total_amount
    }

def summarize_delivered_orders(orders_collection: Collection) -> dict:
    """
    Counts the number of orders with deliveryStatus = "Delivered" and sums the amount for these orders.
    
    Args:
        orders_collection (Collection): The MongoDB orders collection.
        
    Returns:
        dict: A dictionary containing the count of orders and the total amount.
    """
    # MongoDB query to match orders with deliveryStatus = "Delivered"
    query = {"deliveryStatus": "Delivered"}
    
    # Count the number of matching documents
    count = orders_collection.count_documents(query)
    
    # Aggregate to calculate the total amount
    pipeline = [
        {"$match": query},  # Filter documents matching the query
        {"$group": {"_id": None, "totalAmount": {"$sum": "$amount"}}}  # Sum the amount field
    ]
    
    # Execute the aggregation pipeline
    result = list(orders_collection.aggregate(pipeline))
    total_amount = result[0]["totalAmount"] if result else 0
    
    return {
        "count": count,
        "totalAmount": total_amount
    }
