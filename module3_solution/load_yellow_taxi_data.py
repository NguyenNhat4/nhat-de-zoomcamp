import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient
import time

# pip install azure-storage-blob

# Change this to your Azure container name and storage account details
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=modul3zoomcamp;AccountKey=b1AlzL7SRlAT1TPYtZFKnIy4Ory+hXo7CJ7dvCYJMgK1pttU3+VHi+sTNfwh+kxTltiU8fnFqyFR+AStN1Nk3w==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "dezoomcamp-hw3-2025"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Initialize Azure Blob client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

try:
    container_client.create_container()
except Exception as e:
    print(f"Container already exists or error creating container: {e}")


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    
    if os.path.exists(file_path):
        print(f"File already exists: {file_path}, skipping download.")
        return file_path
    
    
    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_azure_upload(blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.exists()


def upload_to_azure(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob_client = container_client.get_blob_client(blob_name)
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to Azure Blob Storage (Attempt {attempt + 1})...")
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True, blob_type="BlockBlob")
            print(f"Uploaded: {blob_client.url}")
            
            if verify_azure_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to Azure: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    # run 4 thread at the same time
    with ThreadPoolExecutor(max_workers=6) as executor:   
        file_paths = list(executor.map(download_file, MONTHS)) 
    # run 4 thread at the same time

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(upload_to_azure, filter(None, file_paths))  

    print("All files processed and verified.")
