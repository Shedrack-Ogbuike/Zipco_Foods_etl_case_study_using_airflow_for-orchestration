import pandas as pd
import os

def run_transformation():
    """
    This function extracts data from a CSV file, performs data cleaning and transformation,
    and prints the status of the operations.
    """
    try:
        # Load the data
        data = pd.read_csv('/home/wonder1844/airflow/zipco_food_dag/zipco_transaction.csv')
        print("Data Extracted Successfully.")
        
        # data cleaning and transformation
        # Remove duplicates
        data.drop_duplicates(inplace=True)
        
        # Handling missing values for numeric columns
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        for column in numeric_columns:
            data[column] = data[column].fillna(data[column].mean())
        
        # Handling missing values for categorical columns
        categorical_columns = data.select_dtypes(include=['object']).columns
        for column in categorical_columns:
            data[column] = data[column].fillna('Unknown', inplace=True)
        
        # Convert 'Date' column to datetime format
        data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
        
        # creating dimensin  and fact tables
        # Create the product table
        products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
        products.index.name = 'ProductID'
        products.reset_index(inplace=True)
        
        # Create the customer table
        customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
        customers.index.name = 'CustomerID'
        customers.reset_index(inplace=True)
        
        # Create the staff table
        staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
        staff.index.name = 'StaffID'
        staff.reset_index(inplace=True) 
        
         # Create transactions tables
        transactions = data.merge(products, on='ProductName', how='left') \
                            .merge(customers, on='CustomerName', how='left') \
                            .merge(staff, on='Staff_Name', how='left')
    
        transactions.index.name = 'TransactionID'
        transactions= transactions.reset_index()[['TransactionID', 'ProductID', 'CustomerID', 'StaffID', 'Date', 'Quantity', 'UnitPrice','StoreLocation','PaymentType', \
                           'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                           'DeliveryTime_min','OrderType', 'DayOfWeek','TotalSales']] 
        
        # Save the cleaned data to CSV files
        data.to_csv('/home/wonder1844/airflow/zipco_food_dag/cleaned_data.csv', index=False)
        products.to_csv('/home/wonder1844/airflow/zipco_food_dag/products.csv', index=False)
        customers.to_csv('/home/wonder1844/airflow/zipco_food_dag/customers.csv', index=False)
        staff.to_csv('/home/wonder1844/airflow/zipco_food_dag/staff.csv', index=False)
        transactions.to_csv('/home/wonder1844/airflow/zipco_food_dag/transactions.csv', index=False)                                                                                                                  
        
        print("Data  cleaning and Transformation Completed Successfully.")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        
        