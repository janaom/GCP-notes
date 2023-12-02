from google.cloud import bigquery

# Path to the service account JSON file
SERVICE_ACCOUNT_JSON = r'C:\Users\\Downloads\bigquery-demo-285417-04f3be542a02.json'

# Create a BigQuery client object using the service account JSON file
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Set the ID of the table to create
table_id = "project.dataset.table"

# Define the schema for the table
schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("count", "INTEGER")
]

# Configure the job for loading data into the table
job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True
)

# Path to the source file to load into the table
file_path = r'F:\record\Big Query\codes and data\names\yob1880.txt'  #data format: Mary,F,7065

# Open the source file
source_file = open(file_path, "rb")

# Load the data from the file into the table using the job configuration
job = client.load_table_from_file(source_file, table_id, job_config=job_config)

# Wait for the job to complete
job.result()

# Retrieve the table object from BigQuery
table = client.get_table(table_id)

# Print the number of rows and columns loaded into the table
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
