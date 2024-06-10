import pandas as pd
import sqlite3

def extract_data(file_path, password):
    return pd.read_excel(file_path, engine='openpyxl')


df_a = extract_data('order_region_a.xlsx', 'order_region_a')
df_b = extract_data('order_region_b.xlsx', 'order_region_b')


def transform_data(df, region):
    df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
    df['region'] = region
    return df

df_a_transformed = transform_data(df_a, 'A')
df_b_transformed = transform_data(df_b, 'B')

df_combined = pd.concat([df_a_transformed, df_b_transformed])

df_combined.drop_duplicates(subset=['OrderId'], inplace=True)



def load_data_to_sqlite(df, db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales_data (
            OrderId INTEGER PRIMARY KEY,
            OrderItemId INTEGER,
            QuantityOrdered INTEGER,
            ItemPrice REAL,
            PromotionDiscount REAL,
            total_sales REAL,
            region TEXT
        )
    ''')

    # Insert data
    df.to_sql('sales_data', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()



load_data_to_sqlite(df_combined, 'sales_data.db')


def execute_query(db_name, query):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results



query_total_records = "SELECT COUNT(*) FROM sales_data"
total_records = execute_query('sales_data.db', query_total_records)


query_total_sales_by_region = "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region"
total_sales_by_region = execute_query('sales_data.db', query_total_sales_by_region)


query_avg_sales_per_transaction = "SELECT AVG(total_sales) FROM sales_data"
avg_sales_per_transaction = execute_query('sales_data.db', query_avg_sales_per_transaction)

query_no_duplicate_ids = "SELECT COUNT(DISTINCT OrderId) FROM sales_data"
no_duplicate_ids = execute_query('sales_data.db', query_no_duplicate_ids)

print("Total number of records:", total_records)
print("Total sales amount by region:", total_sales_by_region)
print("Average sales amount per transaction:", avg_sales_per_transaction)
print("No duplicate OrderIds:", no_duplicate_ids == total_records)