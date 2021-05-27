from pydantic.main import BaseModel
import pytest

pytestmark = pytest.mark.asyncio

from rpc.rpcfactory import RpcFactory, RpcProvider, endpoint

class Input(BaseModel):
  arg: str

class Return(BaseModel):
  value: bytes

class MyRpc:
  rpc: RpcProvider

  @endpoint
  async def func1(self, arg1:str, data: Input, other:str = None) -> Return:
    print('Received', arg1, data, other)
    value: bytes = ''.join(list(reversed(data.arg))).encode()
    ret: Return = Return(value=value)
    print('Returned', ret)
    return ret


async def test_rpc_factory_1():
  factory = RpcFactory()
  client = factory.get_client(MyRpc)
  server = factory.get_server(MyRpc)
  await client.rpc.connect()
  print('client connected')
  await server.rpc.connect()
  print('server connected')

  ret = await client.func1('abc', Input(arg='123'), other=1)

  await client.rpc.disconnect()
  await server.rpc.disconnect()
