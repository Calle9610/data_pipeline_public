import os
import shutil
import pandas as pd

def load_csv(file_path: str, processed_folder: str) -> pd.DataFrame:
    """Loads a CSV file into a pandas DataFrame and moves the file to the processed folder."""
    try:
        # Load the CSV into a DataFrame
        df = pd.read_csv(file_path)
        
        # TODO: uncomment
        # Ensure the processed folder exists, create if it doesn't
        if not os.path.exists(processed_folder):
            os.makedirs(processed_folder)
        
        # Move the file to the processed folder
        file_name = os.path.basename(file_path)
        new_path = os.path.join(processed_folder, file_name)
        shutil.move(file_path, new_path)
        
        print(f"File {file_name} moved to {processed_folder}")
        return df
    except Exception as e:
        raise ValueError(f"Failed to load and process data from {file_path}: {e}")

