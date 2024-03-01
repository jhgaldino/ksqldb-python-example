import requests
import json

KSQLDB_SERVER = 'http://localhost:8088'  

headers = {
    "Content-Type": "application/vnd.ksql.v1+json",
    "Accept": "application/vnd.ksql.v1+json"
}

def execute_ksql(ksql_statement):
    payload = {
        "ksql": ksql_statement,
        "streamsProperties": {}
    }
    response = requests.post(f"{KSQLDB_SERVER}/ksql", headers=headers, data=json.dumps(payload))
    return response.json()

create_stream_statement = """
CREATE STREAM logs_stream (timestamp VARCHAR, log_level VARCHAR, message VARCHAR)
WITH (KAFKA_TOPIC='logs', PARTITIONS=1, VALUE_FORMAT='JSON');
"""


response = execute_ksql(create_stream_statement)
print(response)
