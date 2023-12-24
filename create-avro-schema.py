from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub import SchemaServiceClient
from google.pubsub_v1.types import Schema

# TODO(developer): Replace these variables before running the sample.
project_id = "<...>"
schema_id = "<...>"
avsc_file = "schema.avsc"

project_path = f"projects/{project_id}"

# Read a JSON-formatted Avro schema file as a string.
with open(avsc_file, "rb") as f:
    avsc_source = f.read().decode("utf-8")

schema_client = SchemaServiceClient()
schema_path = schema_client.schema_path(project_id, schema_id)
schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=avsc_source)

try:
    result = schema_client.create_schema(
        request={"parent": project_path, "schema": schema, "schema_id": schema_id}
    )
    print(f"Created a schema using an Avro schema file:\n{result}")
except AlreadyExists:
    print(f"{schema_id} already exists.")
