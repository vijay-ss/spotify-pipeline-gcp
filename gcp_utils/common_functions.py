import os
import ast
import yaml
from google.cloud import storage
from google.cloud import secretmanager

def credentials_accessor():
    try:
        print("Attempting local import...")
        with open('.env.yml') as file:
            payload = yaml.safe_load(file)
        credential_path = payload.get('CREDENTIALS_PATH')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
        print("Local import successful.")

        return payload

    except Exception as error:
        print(error)
        print("Local import failed.")

        print("Attempting GCP import...")
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": "projects/220231394371/secrets/credentials/versions/latest"})
        payload = ast.literal_eval(response.payload.data.decode("UTF-8"))
        print("GCP import successful.")
        
        return payload


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}"
    )