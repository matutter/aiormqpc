from typing import List, Type

from pydantic.main import BaseModel
from rpc import models
from rpc.models import Config, ConfigResult
from _pytest.fixtures import SubRequest
import pytest
from rpc.client import RpcClient, RpcServer

pytestmark = pytest.mark.asyncio

@pytest.fixture
def all_models() -> List[Type[BaseModel]]:
  return [Config, ConfigResult]

@pytest.fixture
async def client(all_models) -> RpcClient:
  cli: RpcClient = RpcClient(models=all_models)
  await cli.connect()
  yield cli
  await cli.disconnect()


@pytest.fixture
async def server(all_models) -> RpcClient:
  cli: RpcServer = RpcServer(models=all_models)
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
