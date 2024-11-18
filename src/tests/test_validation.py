from .. validation import validate_data
import pandas as pd
import pytest

def test_missing_columns():

    # Create a sample DataFrame without the critical column "productId"
    data = {
        "quantity": [5, 10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Expect a ValueError
    with pytest.raises(ValueError, match="Missing required columns: productId"):
        validate_data(df, critical_columns)


def test_missing_values():

    # Create a DataFrame with missing values in critical fields
    data = {
        "productId": ["A1", "B2", None],
        "quantity": [5, 10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Expect a ValueError
    with pytest.raises(ValueError, match="1 rows have missing critical values."):
        validate_data(df, critical_columns)

def test_non_integer_quantity():

    # Create a DataFrame where quantity is a float
    data = {
        "productId": ["A1", "B2", "C3"],
        "quantity": [5.5, 10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Expect a ValueError
    with pytest.raises(ValueError, match="'quantity' must be an integer."):
        validate_data(df, critical_columns)

def test_negative_quantity():

    # Create a DataFrame with a negative quantity
    data = {
        "productId": ["A1", "B2", "C3"],
        "quantity": [5, -10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Expect a ValueError
    with pytest.raises(ValueError, match="'quantity' cannot be negative."):
        validate_data(df, critical_columns)


def test_empty_product_id():

    # Create a DataFrame with an empty productId
    data = {
        "productId": ["A1", " ", "C3"],
        "quantity": [5, 10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Expect a ValueError
    with pytest.raises(ValueError, match="Some rows have empty 'productId'."):
        validate_data(df, critical_columns)


def test_valid_data():

    # Create a DataFrame with valid data
    data = {
        "productId": ["A1", "B2", "C3"],
        "quantity": [5, 10, 15],
    }
    df = pd.DataFrame(data)

    # Define critical columns
    critical_columns = ["productId", "quantity"]

    # Assert that the function returns True
    assert validate_data(df, critical_columns) is None
