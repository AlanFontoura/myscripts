import pyodbc
import pandas as pd

dsn_name = "gresham_staging"

try:
    connection = pyodbc.connect(f"DSN={dsn_name}")
    # cursor = connection.cursor()

    # cursor.execute("SELECT * FROM fees_feeschedule;")
    # rows = cursor.fetchall()

    # for row in rows:
    # print(row)

    query = "SELECT * FROM fees_feeschedule;"
    df = pd.read_sql(query, connection)
    print(df)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # if cursor:
    # cursor.close()
    if connection:
        connection.close()
