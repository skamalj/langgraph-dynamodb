# @! create test cases using pytest for included code include=./src/langgraph_store_dynamodb/dynamodbStore.py , this will use actual dynamodb instance named test_table

import pytest
from boto3.dynamodb.conditions import Key
from langgraph_store_dynamodb.dynamodbStore import DynamoDBStore
from langgraph.store.base import PutOp

@pytest.fixture(scope='module')
def dynamodb_store():
    return DynamoDBStore(table_name='test_table')


def test_put_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    value = {'data': 'value1'}
    dynamodb_store.put(namespace, key, value)
    item = dynamodb_store.get(namespace, key)
    assert item is not None
    assert item.value == value


def test_get_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    item = dynamodb_store.get(namespace, key)
    assert item is not None
    assert item.key == key


def test_delete_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    dynamodb_store.delete(namespace, key)
    item = dynamodb_store.get(namespace, key)
    assert item is None


def test_search_items(dynamodb_store):
    namespace = ('test',)
    key1 = 'item1'
    key2 = 'item2'
    value1 = {'data': 'value1'}
    value2 = {'data': 'value2'}
    dynamodb_store.put(namespace, key1, value1)
    dynamodb_store.put(namespace, key2, value2)
    items = dynamodb_store.search(namespace)
    assert len(items) >= 2
    keys = [item.key for item in items]
    assert key1 in keys
    assert key2 in keys


@pytest.mark.asyncio
async def test_async_get_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    item = await dynamodb_store.aget(namespace, key)
    assert item is not None
    assert item.key == key

@pytest.mark.asyncio
async def test_async_put_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    value = {'data': 'value1'}
    await dynamodb_store.aput(namespace, key, value)
    item = await dynamodb_store.aget(namespace, key)
    assert item is not None
    assert item.value == value

@pytest.mark.asyncio
async def test_async_delete_item(dynamodb_store):
    namespace = ('test',)
    key = 'item1'
    await dynamodb_store.adelete(namespace, key)
    item = await dynamodb_store.aget(namespace, key)
    assert item is None

@pytest.mark.asyncio
async def test_async_batch_operations(dynamodb_store):
    namespace = ('test',)
    key1 = 'item1'
    key2 = 'item2'
    value1 = {'data': 'value1'}
    value2 = {'data': 'value2'}
    ops = [PutOp(namespace, key1, value1), PutOp(namespace, key2, value2)]
    results = await dynamodb_store.abatch(ops)
    assert len(results) == 2
    item1 = await dynamodb_store.aget(namespace, key1)
    item2 = await dynamodb_store.aget(namespace, key2)
    assert item1 is not None
    assert item2 is not None