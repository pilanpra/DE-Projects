import os
from google.cloud import storage
from google.auth import default
from google.auth.credentials import AnonymousCredentials

def upload_files_in_directory_to_gcs(directory_path, gcs_uri_prefix):
    """
    Uploads all files within a directory to Google Cloud Storage.

    Args:
        directory_path (str): Local path to the directory containing files to be uploaded.
        gcs_uri_prefix (str): Destination URI prefix in the format 'gs://bucket-name/path/to/destination/'.

    Returns:
        bool: True if all uploads are successful, False otherwise.
    """
    try:
        # Get credentials
        credentials, _ = default()

        # Create a storage client
        storage_client = storage.Client(project='dm-project-407810', credentials=credentials)

        # Get bucket and blob prefix from the URI prefix
        bucket_name, blob_prefix = parse_gcs_uri_prefix(gcs_uri_prefix)
        blob_prefix += 'BART_DailyRiders'
        
        # Get the bucket
        bucket = storage_client.bucket(bucket_name)
        
        # Iterate over files in the directory
        for foldername in os.listdir(directory_path):
            print(foldername)
            file_path = os.path.join(directory_path, foldername)
            for filename in os.listdir(file_path):
                if filename.endswith('.csv'):  # Process only if it's a CSV file
                    print(filename)
                    # Get the full local file path
                    local_file_path = os.path.join(file_path, filename)
                    # Create a blob object in the bucket
                    # filename_parquet = foldername + '_' + filename
                    blob = bucket.blob(os.path.join(blob_prefix, foldername, filename))
                    # Upload the file to the blob
                    blob.upload_from_filename(local_file_path)

                    print(f"File {local_file_path} uploaded to {blob.name} successfully.")

        return True
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return False

def parse_gcs_uri_prefix(gcs_uri_prefix):
    """
    Parses a Google Cloud Storage URI prefix and returns the bucket name and blob prefix.

    Args:
        gcs_uri_prefix (str): Google Cloud Storage URI prefix in the format 'gs://bucket-name/path/to/prefix/'.

    Returns:
        tuple: (bucket_name, blob_prefix)
    """
    # Remove 'gs://' from the URI and split the string into bucket name and blob prefix
    components = gcs_uri_prefix.replace('gs://', '').split('/', 1)
    bucket_name = components[0]
    blob_prefix = components[1] if len(components) > 1 else ''

    return bucket_name, blob_prefix

# Example usage
if __name__ == "__main__":
    directory_path = "/home/prasadpilankar/transform/RidersMonthly"
    gcs_uri_prefix = "gs://zoomcamp-api-to-gcs"

    upload_files_in_directory_to_gcs(directory_path, gcs_uri_prefix)
