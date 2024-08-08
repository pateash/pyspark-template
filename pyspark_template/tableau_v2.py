import json
from pathlib import Path

import requests


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
    input_file_path = str(Path(__file__).parent.parent / "data" / "people-example.csv")
    tableau_server_url = "https://prod-apnortheast-a.online.tableau.com/"  # Update with your Tableau Server URL
    api_version = "3.8"  # Update with the appropriate API version
    site_id = "ashishebdd926359"  # Leave empty for the default site
    project_id = "Samples"  # Update with your project ID
    # token is enough
    tableau_token = "hPlAOP5+Tual3/AZbZxb/g==:FsCiuVENhosjEYhifAOeiYdmYmzrEEyq"
    datasource_name = "Processed Data"  # Name of the data source

    publish_to_tableau(tableau_server_url, api_version, site_id, project_id, datasource_name, tableau_token,
                       input_file_path)


if __name__ == "__main__":
    main()
