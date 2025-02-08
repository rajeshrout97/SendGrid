import os
import time
import json
import random
import logging
import requests
from datetime import datetime, timedelta
from urllib.parse import quote
from dateutil import parser
from azure.storage.blob import BlobServiceClient

# Configuring Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_API_BASE_URL = os.getenv('SENDGRID_API_BASE_URL')
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
AZURE_CONTAINER_NAME = os.getenv('SENDGRID_BONZO_AZURE_CONTAINER', 'bonzo')
AZURE_FOLDER_NAME = os.getenv('SENDGRID_BONZO_AZURE_FOLDER_NAME', 'sendgrid_data')
SENDGRID_START_DATE = os.getenv('SENDGRID_API_START_DATE')
SENDGRID_END_DATE = os.getenv('SENDGRID_API_END_DATE', '')

INTERVAL = timedelta(minutes=30)
DELAY = 5
RETRY_DELAY = 10
MAX_RETRY_DELAY = 60

# Initialize Blob Service
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

def convert_to_naive(dt):
    return dt.replace(tzinfo=None) if dt.tzinfo else dt

def fetch_data(start_time, end_time):
    query = (f'last_event_time BETWEEN TIMESTAMP "{start_time.isoformat()}Z" '
             f'AND TIMESTAMP "{end_time.isoformat()}Z"')
    encoded_query = quote(query)
    url = f'{SENDGRID_API_BASE_URL}?limit=1000&query={encoded_query}'
    headers = {'Authorization': f'Bearer {SENDGRID_API_KEY}'}
    
    retry_count = 0
    max_retries = 7
    
    while retry_count < max_retries:
        try:
            response = requests.get(url, headers=headers, verify=False)
            
            if response.status_code in [429, 500]:  # Handle rate limits and server errors
                retry_delay = min(RETRY_DELAY * (2 ** retry_count) + random.uniform(0, 1), MAX_RETRY_DELAY)
                logging.warning(f'Error {response.status_code}. Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)
                retry_count += 1
                continue
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f'Error fetching data: {e}')
            retry_count += 1
            time.sleep(RETRY_DELAY * retry_count)
    
    raise Exception('Max retries reached, failed to fetch data.')

def upload_to_azure(blob_name, data):
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data, indent=4), overwrite=True)
    logging.info(f'Uploaded data to {blob_name}')

def fetch_and_upload_data():
    if not SENDGRID_START_DATE:
        raise Exception('SENDGRID_API_START_DATE is blank. Process failed.')
    
    current_time = convert_to_naive(parser.parse(SENDGRID_START_DATE))
    end_date = convert_to_naive(parser.parse(SENDGRID_END_DATE)) if SENDGRID_END_DATE else datetime.utcnow()
    
    temp_interval = INTERVAL
    global_end_time = current_time + INTERVAL
    
    while current_time < end_date:
        next_time = current_time + temp_interval
        next_time = min(next_time, end_date)
        
        data = fetch_data(current_time, next_time)
        if data.get('messages'):
            blob_name = f"{AZURE_FOLDER_NAME}/sendgrid_data_{current_time.strftime('%Y%m%d_%H%M%S%f')}.json"
            upload_to_azure(blob_name, data)
        
        if len(data.get('messages', [])) == 1000:
            temp_interval = max(temp_interval / 2, timedelta(minutes=2))
        else:
            temp_interval = INTERVAL
        
        current_time = next_time
        global_end_time += INTERVAL
        time.sleep(DELAY)
    
    new_start_date = current_time + timedelta(milliseconds=1)
    os.environ['SENDGRID_API_START_DATE'] = new_start_date.isoformat()
    os.environ['SENDGRID_API_END_DATE'] = ''
    logging.info(f'Updated start date to {new_start_date}')

if __name__ == '__main__':
    fetch_and_upload_data()
