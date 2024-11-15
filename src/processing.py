import pandas as pd

def combine_orders_inventory(orders: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Merges orders and inventory data on product_id."""
    return pd.merge(orders, inventory, on="productId", how="left")