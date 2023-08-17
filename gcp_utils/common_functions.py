import os
import ast
import yaml
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import resourcemanager_v3


def get_project_number(project_id: str) -> str:
    """Given a project id, return the project number"""
    client = resourcemanager_v3.ProjectsClient()
    request = resourcemanager_v3.SearchProjectsRequest(query=f"id:{project_id}")
    page_result = client.search_projects(request=request)

    for response in page_result:
        if response.project_id == project_id:
            project = response.name

            return project.replace('projects/', '')


def get_credentials():
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
        project_number = get_project_number(os.environ.get("GOOGLE_CLOUD_PROJECT"))
        response = client.access_secret_version(request={"name": f"projects/{project_number}/secrets/credentials/versions/latest"})
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