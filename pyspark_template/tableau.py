from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import json
import os

# Initialize Spark session
spark = SparkSession.builder.appName("PublishToTableauServer").getOrCreate()

# Function to read data
def read_data(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Function to process data (example transformation)
def process_data(df):
    return df.select("column1", "column2")  # Example: select specific columns

# Function to write data to CSV
def write_data(df, output_path):
    df.write.csv(output_path, header=True, mode="overwrite")

# Function to get Tableau authentication token
def get_tableau_auth_token(tableau_server_url, api_version, tableau_username, tableau_password, site_id):
    auth_url = f"{tableau_server_url}/api/{api_version}/auth/signin"
    auth_payload = {
        "credentials": {
            "name": tableau_username,
            "password": tableau_password,
            "site": {
                "contentUrl": site_id
            }
        }
    }
    auth_headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(auth_url, headers=auth_headers, json=auth_payload)
    response_json = response.json()
    token = response_json["credentials"]["token"]
    site_id = response_json["credentials"]["site"]["id"]
    user_id = response_json["credentials"]["user"]["id"]
    return token, site_id, user_id

# Function to publish data to Tableau Server
def publish_to_tableau(tableau_server_url, api_version, site_id, project_id, datasource_name, token, file_path):
    publish_url = f"{tableau_server_url}/api/{api_version}/sites/{site_id}/datasources"
    publish_headers = {
        "Content-Type": "multipart/mixed",
        "X-Tableau-Auth": token
    }
    publish_payload = {
        "request_payload": json.dumps({
            "datasource": {
                "name": datasource_name,
                "project": {
                    "id": project_id
                }
            }
        })
    }
    with open(file_path, "rb") as file:
        files = {
            "request_payload": (None, publish_payload["request_payload"], "application/json"),
            "tableau_datasource": (file_path, file, "text/csv")
        }
        response = requests.post(publish_url, headers=publish_headers, files=files)
    if response.status_code == 201:
        print("Data source published successfully.")
    else:
        print(f"Failed to publish data source. Status code: {response.status_code}")
        print(response.text)

# Main function
def main():
    # Define file paths and Tableau Server credentials
    input_file_path = "path/to/your/input.csv"  # Update with your input file path
    output_file_path = "/tmp/processed_data.csv"
    tableau_server_url = "https://your-tableau-server-url"  # Update with your Tableau Server URL
    api_version = "3.8"  # Update with the appropriate API version
    site_id = ""  # Leave empty for the default site
    project_id = "your_project_id"  # Update with your project ID
    tableau_username = "your_username"  # Update with your Tableau username
    tableau_password = "your_password"  # Update with your Tableau password
    datasource_name = "Processed Data"  # Name of the data source

    # Read, process, and write data
    df = read_data(input_file_path)
    processed_df = process_data(df)
    write_data(processed_df, output_file_path)

    # Authenticate and publish data
    token, site_id, user_id = get_tableau_auth_token(tableau_server_url, api_version, tableau_username, tableau_password, site_id)
    publish_to_tableau(tableau_server_url, api_version, site_id, project_id, datasource_name, token, output_file_path)

    # Clean up
    os.remove(output_file_path)

if __name__ == "__main__":
    main()

# Stop Spark session
spark.stop()
