import psycopg2
import csv
import os
import json

def connect_to_database():
    try:
        connection = psycopg2.connect(
            host="192.168.12.62",
            database="RMS",
            user="postgres",
            password="Admin@4321"
        )
        print("Connected to the database")
        return connection
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database")
        print(e)
        return None

def fetch_data(connection, query):
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()  # Fetch all rows returned by the query
        return rows
    except psycopg2.Error as e:
        print("Error: Unable to fetch data from the database")
        print(e)
        return None
    finally:
        cursor.close()

def get_table_names(connection):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT LIKE 'django\_%'"
    return fetch_data(connection, query)

def create_csv_and_json_for_table(table_name, columns, data, related_data):
    file_name_csv = f"RMS_APP_DATA/CSV/{table_name}.csv"
    file_name_json = f"RMS_APP_DATA/JSON/{table_name}.json"
    
    with open(file_name_csv, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(columns)  # Write the column headers
        csv_writer.writerows(data)     # Write the data rows
    print(f"CSV file '{file_name_csv}' created")
    
    json_data = {}
    for row in data:
        id_column_index = columns.index("id")
        json_row = {columns[i]: str(value) for i, value in enumerate(row)}
        json_row["id"] = str(row[id_column_index])  # Include the 'id' column in the value
        json_data[str(row[id_column_index])] = json_row
    
    with open(file_name_json, "w") as json_file:
        json.dump(json_data, json_file, indent=4)
    print(f"JSON file '{file_name_json}' created")


def main():
    connection = connect_to_database()
    
    if connection:
        try:
            table_names = get_table_names(connection)
            if table_names:
                print("Tables in the database:")
                for name in table_names:
                    table_name = name[0]
                    query = f"SELECT * FROM {table_name}"
                    table_data = fetch_data(connection, query)
                    columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                    column_names = [col[0] for col in fetch_data(connection, columns_query)]

                    if table_name == "userconfig":
                        # Fetch combined data using a JOIN query
                        query = f"""
                            SELECT  gc.*,uc.id as userconfig_id
                            FROM userconfig uc
                            JOIN groupconfig gc 
                            ON uc.group_config_id = gc.id
                        """
                        combined_data = fetch_data(connection, query)
                        print('**********************',list(combined_data))
                    else:
                        combined_data = None

                    create_csv_and_json_for_table(table_name, column_names, table_data, combined_data)
            
            connection.commit()
        finally:
            connection.close()
            print("Connection closed")

            
if __name__ == "__main__":
    main()