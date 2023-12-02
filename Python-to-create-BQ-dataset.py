from google.cloud import bigquery

# Path to the service account JSON file
SERVICE_ACCOUNT_JSON = 'C:\Users\\Downloads\bigquery-demo-285417-04f3be542a02.json'

# Create a BigQuery client object using the service account JSON file
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Specify the ID of the dataset to create
dataset_id = "project.dataset"

# Create a Dataset object with the specified dataset ID
dataset = bigquery.Dataset(dataset_id)

# Set the geographic location where the dataset should reside
dataset.location = "US"

# Set a description for the dataset
dataset.description = "my new dataset"

# Send the dataset to the API for creation, with an explicit timeout
# Raises google.api_core.exceptions.Conflict if the dataset already exists within the project
dataset_ref = client.create_dataset(dataset, timeout=30)  # Make an API request

# Print a message indicating the successful creation of the dataset
print("Created dataset {}.{}".format(client.project, dataset_ref.dataset_id))
