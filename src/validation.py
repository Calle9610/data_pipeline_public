import pandas as pd

def validate_data(df: pd.DataFrame, critical_columns: list) -> bool:
    """
    Validate the data in the DataFrame to ensure it is clean and correct.
    
    Args:
        df (pd.DataFrame): The DataFrame to validate.
        
    Returns:
        bool: True if data is valid, False otherwise.
    """
    # 1. Ensure required columns are present
    missing_columns = [col for col in critical_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # 2. Ensure there are no missing values in critical fields
    missing_values = df[critical_columns].isnull().any(axis=1).sum()
    
    if missing_values > 0:
        raise ValueError(f"{missing_values} rows have missing critical values.")
    
    # 3. Ensure the 'quantity' is a positive integer
    if not pd.api.types.is_integer_dtype(df['quantity']):
        raise ValueError("'quantity' must be an integer.")
    
    if any(df['quantity'] < 0):
        raise ValueError("'quantity' cannot be negative.")
    
    # 4. Ensure key' are non-empty
    if any(df[critical_columns[0]].str.strip() == ''):
        raise ValueError("Some rows have empty 'productId'.")
    

def remove_dublicates(df: pd.DataFrame, keys: list, collection_name: str):
        print(f"{collection_name} count BEFORE cleaning duplicates: {df.shape[0]}")
        df = df.drop_duplicates(subset=keys)
        print(f"{collection_name} count AFTER cleaning duplicates: {df.shape[0]}")
        return df
    
    