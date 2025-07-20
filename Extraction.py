import pandas as pd
import os

# data extraction

def run_extraction():
    try:
        data = pd.read_csv('zipco_transaction.csv')
        print("Data Extracted Successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
