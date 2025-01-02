import boto3
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, Iterator
from contextlib import contextmanager
from botocore.exceptions import ClientError
from langgraph.store.base import BaseStore, Item, PutOp, Result, SearchItem, NamespacePath, Literal, Op, SearchOp
from datetime import datetime
import asyncio
import aioboto3



class DynamoDBStore(BaseStore):
    def __init__(self, table_name: str,  max_read_request_units: int = 10, max_write_request_units: int = 10):
        super().__init__()
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self._get_or_create_table(table_name, max_read_request_units,max_write_request_units)
        self.table_name = table_name
    
    @classmethod
    @contextmanager
    def from_conn_info(cls, *, table_name: str,  max_read_request_units: int = 10, max_write_request_units: int = 10) -> Iterator["DynamoDBStorer"]:
        store = None
        try:
            store = DynamoDBStore(table_name, max_read_request_units, max_write_request_units)
            yield store
        finally:
            pass
    
    def _get_or_create_table(self, table_name: str, max_read_request_units: int, max_write_request_units: int):
        try:
            # Attempt to load the table
            table = self.dynamodb.Table(table_name)
            table.load()  # This will raise an exception if the table does not exist
            print(f"Table '{table_name}' already exists.")
            return table
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table does not exist, create it
                print(f"Table '{table_name}' not found. Creating table...")
                key_schema = [
                    {'AttributeName': 'PK', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'SK', 'KeyType': 'RANGE'},  # Sort key
                ]
                attribute_definitions = [
                    {'AttributeName': 'PK', 'AttributeType': 'S'},  # String type
                    {"AttributeName": "SK", "AttributeType": 'S'},
                ]
                
                table = self.dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=key_schema,
                    AttributeDefinitions=attribute_definitions,
                    BillingMode='PAY_PER_REQUEST',
                    OnDemandThroughput={
                        'MaxReadRequestUnits': max_read_request_units,
                        'MaxWriteRequestUnits': max_write_request_units
                    }
                )
                table.wait_until_exists()  # Wait for the table to become active
                print(f"Table '{table_name}' created successfully.")
                return table
            else:
                raise  # Re-raise any other exceptions
    from datetime import datetime

    def _map_to_item(self, result_dict, namespace, return_type='Item'):
        # Extract values from the result_dict
        key = result_dict['SK']  # Using 'SK' as the unique key
        value = result_dict['value']
        created_at = result_dict['created_at']
        updated_at = result_dict['updated_at']

        target_class = Item if return_type == 'Item' else SearchItem

        # Create an Item object
        item = target_class(
            value=value,
            key=key,
            namespace=self._deconstruct_namespace(namespace),
            created_at=created_at,
            updated_at=updated_at
        )    
        return item

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        results = []

        for op in ops:
            if isinstance(op, SearchOp):
                # Handle SearchOp
                result = self.search(
                    namespace_prefix=op.namespace_prefix,
                    query=None,
                    filter=None
                )
            elif isinstance(op, PutOp):
                # Handle PutOp
                if op.value is None:
                    # Delete operation
                    result = self.delete(namespace=op.namespace, key=op.key)
                else:
                    # Put operation
                    result = self.put(
                        namespace=op.namespace, key=op.key, value=op.value
                    )
            else:
                raise NotImplementedError(f"Operation type {type(op)} is not supported.")

            # Append the operation result
            results.append(result)

        return results
    
    async def abatch(self, ops: Iterable[Op]) -> List[Result]:
        """
        Asynchronously perform batch operations.
        """
        async def handle_op(op: Op) -> Result:
            if isinstance(op, SearchOp):
                result = await self.asearch(op.namespace_prefix, query=None, filter=None)
            elif isinstance(op, PutOp):
                if op.value is None:
                    result = await self.adelete(namespace=op.namespace, key=op.key)
                else:
                    result = await self.aput(namespace=op.namespace, key=op.key, value=op.value)
            else:
                raise NotImplementedError(f"Operation type {type(op)} is not supported.")
            return result

        results = await asyncio.gather(*(handle_op(op) for op in ops))
        return results



    def get(self, namespace: Tuple[str, ...], key: str) -> Optional[Item]:
        """
        Retrieve an item based on its composite key (namespace, key).
        """
        composite_key = self._construct_composite_key(namespace, key)
        response = self.table.get_item(Key={'PK': composite_key[0], 'SK': composite_key[1]})
        item = response.get('Item')
        if item:
            return self._map_to_item(item, namespace)
        return None
    
    async def aget(self, namespace: Tuple[str, ...], key: str) -> Optional[Item]:
        """
        Asynchronously retrieve an item based on its composite key (namespace, key).
        """
        composite_key = self._construct_composite_key(namespace, key)
        session = aioboto3.Session()
        async with session.resource('dynamodb') as dynamodb:
            table = await dynamodb.Table(self.table_name)
            response = await table.get_item(Key={'PK': composite_key[0], 'SK': composite_key[1]})
            item = response.get('Item')
            if item:
                return self._map_to_item(item, composite_key[0])
        return None

    def search(
        self,
        namespace_prefix: Tuple[str, ...],
        *,
        query: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchItem]:
        """
        Search for items in a given namespace, applying optional query and filter.
        """
        namespace = ':'.join(namespace_prefix)
        
        response = self.table.query(
            ExpressionAttributeValues={
                ':PK': namespace
            },
            KeyConditionExpression='PK = :PK',  
            Limit=limit
        )

        items = response.get('Items', [])
        return [self._map_to_item(item, namespace, 'SearchItem') for item in items]
    
    async def asearch(
        self,
        namespace_prefix: Tuple[str, ...],
        *,
        query: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchItem]:
        """
        Asynchronously search for items in a given namespace, applying optional query and filter.
        """
        namespace = ':'.join(namespace_prefix)

        session = aioboto3.Session()
        async with session.resource('dynamodb') as dynamodb:
            table = await dynamodb.Table(self.table_name)
            response = await table.query(
                ExpressionAttributeValues={':PK': namespace},
                KeyConditionExpression='PK = :PK',
                Limit=limit
            )
            items = response.get('Items', [])
            return [self._map_to_item(item, namespace, 'SearchItem') for item in items]

    def put(
        self,
        namespace: Tuple[str, ...],
        key: str,
        value: Dict[str, Any],
        index: Optional[Union[Literal[False], List[str]]] = None
    ) -> None:
        """
        Insert or update an item in the table.
        """
        composite_key = self._construct_composite_key(namespace, key)
        existing_item = self.get(namespace=composite_key[0], key=composite_key[1])
        current_time = datetime.utcnow().isoformat()
        item = {
            'PK': composite_key[0],
            'SK': composite_key[1],
            'value': value,
            'created_at': existing_item.created_at if existing_item else current_time,
            'updated_at': current_time,
        }
        try:
            self.table.put_item(Item=item)
        except Exception as e:
            print(f"Error putting item into DynamoDB: {e}")
    

    async def aput(
        self,
        namespace: Tuple[str, ...],
        key: str,
        value: Dict[str, Any],
        index: Optional[Union[Literal[False], List[str]]] = None
    ) -> None:
        """
        Asynchronously insert or update an item in the table.
        """
        composite_key = self._construct_composite_key(namespace, key)
        current_time = datetime.utcnow().isoformat()
        
        session = aioboto3.Session()
        async with session.resource('dynamodb') as dynamodb:
            table = await dynamodb.Table(self.table_name)
            item = {
                'PK': composite_key[0],
                'SK': composite_key[1],
                'value': value,
                'created_at': current_time,
                'updated_at': current_time,
            }
            await table.put_item(Item=item)

    def delete(self, namespace: Tuple[str, ...], key: str) -> None:
        """
        Delete an item based on its composite key (namespace, key).
        """
        composite_key = self._construct_composite_key(namespace, key)
        self.table.delete_item(Key={'PK': composite_key[0], 'SK': composite_key[1]})
    
    async def adelete(self, namespace: Tuple[str, ...], key: str) -> None:
        """
        Asynchronously delete an item based on its composite key (namespace, key).
        """
        composite_key = self._construct_composite_key(namespace, key)
        session = aioboto3.Session()
        async with session.resource('dynamodb') as dynamodb:
            table = await dynamodb.Table(self.table_name)
            await table.delete_item(Key={'PK': composite_key[0], 'SK': composite_key[1]})

    def list_namespaces(self, *, prefix: Optional[NamespacePath] = None, 
                    suffix: Optional[NamespacePath] = None, 
                    max_depth: Optional[int] = None, 
                    limit: int = 100, offset: int = 0) -> list[tuple[str, ...]]:
        raise NotImplementedError("The 'list_namespaces' method is not implemented yet.")
    

    def _construct_composite_key(self, namespace: Tuple[str, ...], key: str) -> Tuple[str, str]:
        """
        Combine namespace and key to form a composite key (PK, SK).
        """
        namespace_str = ':'.join(namespace)
        return (namespace_str, key)
    
    def _deconstruct_namespace(self, namespace: str) -> tuple:
        if not namespace:
            return ()

        if ':' in namespace:
            return tuple(namespace.split(':'))
        else:
            return (namespace,)




        
