import requests
import os
from dotenv import load_dotenv
import shopify
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from typing import Optional

load_dotenv()

SHOP_URL = os.getenv("SHOP_URL")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
API_VERSION = os.getenv("API_VERSION")
credentials_path = 'upwork-478017-8be10990e83c.json'

def get_access_token_oauth(shop_url):
    """
    Exchange authorization code for access token (OAuth flow)
    
    Args:
        shop_url: the shop URL (e.g., 'yourstore.myshopify.com')
        authorization_code: Code received from authorization callback
    
    Returns:
        access_token: The access token to use for API calls
    """
    token_url = f"https://{shop_url}/admin/oauth/access_token"
    
    payload = {
        'client_id': API_KEY,
        'client_secret': API_SECRET,
        'grant_type': "client_credentials"
    }
    
    response = requests.post(token_url, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        access_token = data['access_token']
        print(f"Access token obtained successfully!")
        return data, access_token
    else:
        raise Exception(f"Failed to get access token: {response.text}")


def connect_to_shopify(access_token=None):
    """Establish connection to Shopify store"""
    token = access_token
    
    if not token:
        raise ValueError("Access token is required. Please provide an access token.")
    
    # Using Access Token (Recommended for custom apps)
    session = shopify.Session(SHOP_URL, API_VERSION, token)
    shopify.ShopifyResource.activate_session(session)    
    
    print(f"Connected to {SHOP_URL}")


def run_shopifyQL_query(query, access_token=None):
    """Run a ShopifyQL query and return results"""
    url = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }
    
    graphql_query = {
            "query": """
                    query ($qlQuery: String!) 
                    {
                            shopifyqlQuery(query: $qlQuery)
                            {
                            tableData
                            {
                                columns 
                                {
                                    name
                                    dataType
                                    displayName
                                }
                                rows
                            }
                            parseErrors
                            }
                    }""",
            "variables": {
                "qlQuery": query
            }
    }
    
    response = requests.post(url, json=graphql_query, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        result = data.get("data", {}).get("shopifyqlQuery", {})
    
        # parseErrors is now likely a list of strings or a single string
        errors = result.get("parseErrors")
        if errors:
            print("ShopifyQL Errors found:")
            print(errors) 
        else:
            table_data = result.get("tableData", {})        
    else:
        raise Exception(f"GraphQL query failed: {response.text}")

    return table_data


def write_dataframe_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    if_exists: str = 'append',
    credentials_path: Optional[str] = None
) -> None:
    """
    Write a pandas DataFrame to a Google BigQuery table.
    
    Args:
        df: pandas DataFrame to write
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        if_exists: What to do if table exists ('fail', 'replace', 'append')
        credentials_path: Optional path to service account JSON key file
    
    Returns:
        None
    
    Raises:
        ValueError: If if_exists parameter is invalid
        Exception: If write operation fails
    """
    
    # Validate if_exists parameter
    valid_options = ['fail', 'replace', 'append']
    if if_exists not in valid_options:
        raise ValueError(f"if_exists must be one of {valid_options}")
    
    # Initialize BigQuery client
    if credentials_path:
        client = bigquery.Client.from_service_account_json(
            credentials_path,
            project=project_id
        )
    else:
        client = bigquery.Client(project=project_id)
    
    # Construct full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        # Check if dataset exists, create if not
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Change as needed
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}")
        
        # Check if table exists
        try:
            table = client.get_table(table_ref)
            table_exists = True
            print(f"Table {table_id} exists")
            
            if if_exists == 'fail':
                raise ValueError(f"Table {table_ref} already exists and if_exists='fail'")
            elif if_exists == 'replace':
                client.delete_table(table_ref)
                print(f"Deleted existing table {table_id}")
                table_exists = False
        except NotFound:
            table_exists = False
            print(f"Table {table_id} does not exist, will create")
        
        # Configure job settings
        job_config = bigquery.LoadJobConfig()
        
        if not table_exists or if_exists == 'replace':
            # Auto-detect schema for new tables
            job_config.autodetect = True
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:  # append
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        # Write DataFrame to BigQuery
        job = client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        
        # Wait for job to complete
        job.result()
        
        # Get updated table info
        table = client.get_table(table_ref)
        print(f"Successfully loaded {table.num_rows} rows to {table_ref}")
        
    except Exception as e:
        print(f"Error writing to BigQuery: {str(e)}")
        raise
