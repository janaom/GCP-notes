from google.cloud import bigquery

# Path to the service account JSON file
SERVICE_ACCOUNT_JSON = r'C:\Users\\Downloads\bigquery-demo-285417-04f3be542a02.json'

# Create a BigQuery client object using the service account JSON file
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Define the SQL query to run
query_to_run = "select * from project.dataset.table"

# Run the query using the client
query_job = client.query(query_to_run)

# Print the query job object
print(query_job)

# Print a message indicating that the script ran
print("Script ran")

# Iterate over the results of the query and print each row
for row in query_job:
    print(str(row[0]) + "," + str(row[1]) + "," + str(row[2]))
