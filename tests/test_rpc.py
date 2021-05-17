import sys
import logging
from typing import List, Type

from pydantic.main import BaseModel
from rpc import models
from rpc.models import BinaryObj, Config, ConfigResult, NestedObject
from _pytest.fixtures import SubRequest
import pytest
from rpc.client import RpcClient, RpcServer

pytestmark = pytest.mark.asyncio
logging.getLogger('aiormq.connection').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
import coloredlogs

coloredlogs.install(logging.DEBUG)

@pytest.fixture
def unloaded_models():
  if 'rpc.models' in sys.modules:
    del sys.modules['rpc.models']
    print('delete python module cache')


@pytest.fixture
async def client() -> RpcClient:
  cli: RpcClient = RpcClient()
  await cli.connect()
  yield cli
  await cli.disconnect()


@pytest.fixture
async def server() -> RpcClient:
  cli: RpcServer = RpcServer()
  await cli.connect()
  yield cli
  await cli.disconnect()


async def test_connect(client: RpcClient, server: RpcServer):
  assert client.is_connected
  assert server.is_connected


async def test_basic_rpc_1(client: RpcClient, server: RpcServer):
  print('client', client.id, 'server', server.id)

  async def my_callback(conf:Config) -> ConfigResult:
    print('received', conf)
    result = ConfigResult(status_code=200)
    print('sending', result)
    return result

  server.add_method(my_callback)
  server.add_method(my_callback, name='test')
  config: Config = Config(id='xxx', secret='yyy')
  result: ConfigResult = await client.call('test', config)
  assert result.status_code == 200
  result: ConfigResult = await client.call('my_callback', config)
  assert result.status_code == 200


async def test_basic_rpc_binary_data(client: RpcClient, server: RpcServer, unloaded_models):
  print('client', client.id, 'server', server.id)

  async def my_callback(conf:Config) -> bytes:
    print('received', conf)
    result = conf.secret.encode()
    print('sending', result)
    return result

  server.add_method(my_callback)
  config: Config = Config(id='xxx', secret='yyy')
  result: ConfigResult = await client.call('my_callback', config)
  assert result == b'yyy'

  async def bin_callback(data: BinaryObj) -> BinaryObj:
    print('received', data)
    data.data = b'bbb\0\0\0'
    result = data
    print('sending', result)
    return result

  server.add_method(bin_callback)
  data: BinaryObj = BinaryObj(id='xxx', data=b'aaa')
  result: BinaryObj = await client.call('bin_callback', data)
  assert result.data == b'bbb\0\0\0'

async def test_msgpack():
  import msgpack
  conf = BinaryObj(id='xxx', data=b'\0yyy')
  d = conf.dict()
  print(d)
  raw = msgpack.packb(d)
  print(raw)
  o = BinaryObj.parse_obj(msgpack.unpackb(raw))
  print(o)


async def test_basic_rpc_complex_data_1(client: RpcClient, server: RpcServer):
  print('client', client.id, 'server', server.id)

  async def my_callback(obj: NestedObject) -> bytes:
    print('received', obj)
    obj.id = 'updated'
    result = obj
    print('sending', result)
    return result

  server.add_method(my_callback)
  obj = NestedObject(id='nested', bin=BinaryObj(id='bin', data=b'xxx'), conf=Config(id='conf', secret='yyy'))
  res: NestedObject = await client.call('my_callback', obj)
  assert res.id == 'updated'
