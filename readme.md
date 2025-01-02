# LangGraph DynamoDB Store

A DynamoDB-based store implementation for LangGraph that allows long term memory implementation.

This implements both sync and async methods for [BaseStore](https://langchain-ai.github.io/langgraph/reference/store/#langgraph.store.base.BaseStore)

bash
pip install langgraph-store-dynamodb

## Usage

### Basic Initialization

python
from langgraph_store_dynamodb import DynamoDBStore

# Initialize the store with a table name
store = DynamoDBStore(
    table_name="your-dynamodb-table-name",
    max_read_request_units=10,  # Optional, default is 10
    max_write_request_units=10  # Optional, default is 10
)


### Alternative Initialization Using Context Manager

```
from langgraph_dynamodb_checkpoint import DynamoDBStore

with DynamoDBStore.from_conn_info(table_name="your-dynamodb-table-name") as store:
    # Use the store here
```


## Parameters

### DynamoDBStore Constructor

- `table_name` (str): Name of the DynamoDB table to use for storing checkpoints
- `max_read_request_units` (int, optional): Maximum read request units for the DynamoDB table. Defaults to 10
- `max_write_request_units` (int, optional): Maximum write request units for the DynamoDB table. Defaults to 10

## Table Structure

The store automatically creates a DynamoDB table if it doesn't exist, with the following structure:

- Partition Key (PK): String type, used for namespace
- Sort Key (SK): String type, used for memory key

## AWS Configuration

Ensure you have proper AWS credentials configured either through:
- Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
- AWS credentials file (~/.aws/credentials)
- IAM role when running on AWS services

The AWS credentials should have permissions to:
- Create DynamoDB tables (if table doesn't exist)
- Read and write to DynamoDB tables

## Notes

- The store automatically creates the DynamoDB table if it doesn't exist
- Uses on-demand billing mode for DynamoDB
- Implements methods required by the LangGraph BaseStore interface