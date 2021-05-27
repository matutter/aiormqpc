import asyncio
import logging
from os import wait
from typing import List

import coloredlogs
import pytest
from _pytest.fixtures import SubRequest
from pydantic.main import BaseModel
from rpc import RpcFactory, RpcProvider, endpoint
from rpc.rpcfactory import RpcError

coloredlogs.install(logging.DEBUG)
logging.getLogger('aiormq.connection').setLevel(logging.ERROR)

pytestmark = pytest.mark.asyncio


class Input(BaseModel):
  arg: str


class Obj(BaseModel):
  arg: int


class Return(BaseModel):
  value: bytes


class MyRpc:
  rpc: RpcProvider

  def server_arg(self, arg1):
    print(arg1)

  def server_data(self, data):
    print(data)

  def server_other(self, other):
    print(other)

  def server_o(self, o):
    print(o)

  @endpoint
  async def noreturn(self, arg1: str = None) -> None:
    print('Returned', None)
    return None

  if 0:
    # Not supported (yet)
    @endpoint
    async def returnlist1(self, arg1: str = None) -> List[str]:
      ret = []
      if arg1:
        ret = arg1.split()
      print('Returned', ret)
      return ret

  @endpoint
  async def func1(self, arg1: str, data: Input, other: str = 2, o: Obj = None) -> Return:
    print('Received', arg1, data, other, o)
    self.server_arg(arg1)
    self.server_data(data)
    self.server_other(other)
    self.server_o(o)
    value: bytes = ''.join(list(reversed(data.arg))).encode()
    ret: Return = Return(value=value)
    print('Returned', ret)
    return ret


@pytest.fixture
async def cleanup():
  holder = []
  yield holder.append
  for func in holder:
    await func()

async def test_rpc_factory_1(cleanup):
  factory = RpcFactory()
  client = factory.get_client(MyRpc)
  server = factory.get_server(MyRpc)
  await client.rpc.connect()
  print('client connected')
  await server.rpc.connect()
  print('server connected')

  async def disconnect():
    await client.rpc.disconnect()
    await server.rpc.disconnect()
  cleanup(disconnect)

  ret = await client.func1('abc', Input(arg='123'), other=1)
  assert ret.value == b'321'
  ret = await client.func1('abc', Input(arg='xyz'))
  assert ret.value == b'zyx'
  ret = await client.func1('abc', Input(arg='xyz'))
  assert ret.value == b'zyx'
  ret = await client.func1('abc', Input(arg='321'), o=Obj(arg=123))
  assert ret.value == b'123'
  with pytest.raises(RpcError):
    ret = await client.func1(Input(arg='321'), o=Obj(arg=123))

  ret = await client.func1(None, Input(arg='321'), o=None)
  assert ret.value == b'123'

  ret = await client.noreturn(None)
  assert ret == None

  if 0:
    # Not supported (yet)
    ret = await client.returnlist1('abc')
    assert ret == ['a', 'b', 'c']

async def test_rpc_factory_2():

  with pytest.raises(TypeError):
    class MyUnsupportedType:
      pass

    class AnRpcClass:
      @endpoint
      async def func(var: str, unsupported: MyUnsupportedType):
        pass
