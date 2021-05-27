from asyncio.futures import Future
from inspect import getfullargspec, FullArgSpec

from aiormq import connection
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Type, TypeVar
from aiormq.abc import DeliveredMessage
from aiormq.connection import Connection
import msgpack
from pamqp.commands import Basic, Channel, Queue
from pydantic import BaseModel
from pydantic.fields import Field
from uuid import UUID, uuid1
import aiormq
import asyncio
import logging

T = TypeVar('T')

log = logging.getLogger(__name__)

class RpcMessage(BaseModel):
  id: str = Field(default_factory=lambda: uuid1().hex)
  # queue name to send result
  reply_to: Optional[str]
  # name of the method to call
  name: str
  # client args
  args: List[Any] = Field(default_factory=list)
  # client key word arguments
  kwargs: Dict[str, Any] = Field(default_factory=dict)
  # server result
  result: Optional[Any]


class RpcProvider:
  id: str
  queue_name: str
  connection: Connection
  channel: Channel

  # Client properties
  callback_queue_name: Optional[str]
  callback_timeout: float
  pending_results: Dict[str, Future]

  # Server properties
  endpoints: Dict[str, 'Endpoint']

  def __init__(
    self,
    id:str = None,
    queue_name: str = None,
    callback_timeout: float = 10.0,
    callback_queue_name: str = None,
    connection: Connection = None,
    channel: Channel = None,
    endpoints: Dict[str, 'Endpoint'] = {}) -> None:

    self.id = id or uuid1().hex
    self.queue_name = queue_name or 'rpc-queue'
    self.connection = connection
    self.channel = channel
    self.callback_timeout = callback_timeout
    self.callback_queue_name = callback_queue_name
    self.endpoints = endpoints
    self.pending_results = dict()

  @property
  def is_connected(self) -> bool:
    return bool(self.connection and self.channel)


  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if self.is_connected:
      return
    try:
      if self.connection is None:
        self.connection = await aiormq.connect(dsn)
      self.channel = await self.connection.channel()
    except:
      await self.disconnect()
      raise


  async def disconnect(self) -> None:
    try:
      if self.connection:
        await self.connection.close()
    finally:
      self.connection = None
      self.channel = None


  async def send(self, msg: RpcMessage) -> RpcMessage:
    pass

  async def receive(self, message: DeliveredMessage) -> None:
    pass


class RpcServer(RpcProvider):

  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if not self.is_connected:
      await super().connect(dsn=dsn)

    await self.channel.queue_declare(queue=self.queue_name, exclusive=True)
    await self.channel.basic_consume(self.queue_name, self.receive)

  async def receive(self, message: DeliveredMessage) -> None:
    msg = RpcMessage.parse_obj(msgpack.unpackb(message.body))
    endpoint = self.endpoints[msg.name]
    response: RpcMessage = await endpoint.receive(msg)
    data = msgpack.packb(response.dict())
    await self.channel.basic_publish(data, routing_key=msg.reply_to)

class RpcClient(RpcProvider):

  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if not self.is_connected:
      await super().connect(dsn=dsn)
    declare_ok:Queue.DeclareOk = await self.channel.queue_declare(queue='', exclusive=True)
    self.callback_queue_name = declare_ok.queue
    await self.channel.basic_consume(self.callback_queue_name, self.receive)

  async def send(self, msg: RpcMessage) -> RpcMessage:
    msg.reply_to = self.callback_queue_name
    data = msgpack.packb(msg.dict())
    future = Future()
    self.pending_results[msg.id] = future
    await self.channel.basic_publish(data, routing_key=self.queue_name)
    return await asyncio.wait_for(future, self.callback_timeout)

  async def receive(self, message: DeliveredMessage):
    msg = RpcMessage.parse_obj(msgpack.unpackb(message.body))
    future = self.pending_results[msg.id]
    future.set_result(msg)


class Endpoint:
  arg_models: Dict[int, Type[BaseModel]]
  argspec: FullArgSpec
  attribute: str = '__rpc_endpoint__'
  func: Callable[[Any], Awaitable]
  keyword_models: Dict[str, Type[BaseModel]]
  name: str
  return_type: Type

  # Only required for client side
  provider: Optional[RpcProvider]

  def __init__(self, func: Callable, provider: RpcProvider = None) -> None:
    self.provider = provider
    self.func = func
    self.name = getattr(func, '__name__', str(func))
    self.argspec: FullArgSpec = getfullargspec(func)
    self.return_type = None
    self.arg_models = dict()
    self.keyword_models = dict()

    annotations = dict(self.argspec.annotations)
    args = [a for a in self.argspec.args if a != 'self']

    if 'return' in annotations:
      self.return_type = annotations.pop('return')

      if not issubclass(self.return_type, (BaseModel, str, int, float, str, bytes, bool)):
        raise TypeError(f'unexpected endpoint return type {self.return_type}')

    for i, arg in enumerate(args):
      if arg not in annotations:
        log.warning(f'missing type annotation for argument {arg} of {self.name}')
        continue
      if issubclass(annotations[arg], BaseModel):
        self.arg_models[i] = annotations.pop(arg)
    self.keyword_models = {k:v for k,v in annotations.items() if issubclass(v, BaseModel)}

  async def send(self, *args: Any, **kwargs: Any) -> Any:

    args = list(args)
    for i in self.arg_models.keys():
      if args[i]:
        args[i] = args[i].dict()

    for key in self.keyword_models.keys():
      if kwargs.get(key):
        kwargs[key] = kwargs[key].dict()

    msg = RpcMessage(name=self.name, args=args, kwargs=kwargs)
    response: RpcMessage = await self.provider.send(msg)
    if self.return_type and issubclass(self.return_type, BaseModel):
      result = self.return_type.parse_obj(response.result)
      return result
    return response.result

  async def receive(self, msg: RpcMessage) -> RpcMessage:

    args = list(msg.args)
    for i, cls in self.arg_models.items():
      if args[i]:
        args[i] = cls.parse_obj(args[i])

    kwargs = msg.kwargs
    for key, cls in self.keyword_models.items():
      if kwargs.get(key):
        kwargs[key] = cls.parse_obj(kwargs[key])

    result = await self.func(*args, **kwargs)
    if issubclass(self.return_type, BaseModel):
      result = result.dict()

    response = RpcMessage(id=msg.id, name=msg.name, result=result)
    return response

  @classmethod
  def has_endpoint(cls, obj) -> bool:
    if not obj: return False
    return getattr(obj, cls.attribute, False)

class RpcFactory(RpcProvider):

  def get_endpoints(self, cls: Type, provider: RpcProvider) -> List[Endpoint]:
    endpoints = list()
    for attr in dir(cls):
      func = getattr(cls, attr)
      if not Endpoint.has_endpoint(func):
        continue
      end = Endpoint(func, provider)
      endpoints.append(end)
    return endpoints

  def get_client(self, cls: Type[T], *args, **kwargs) -> T:
    if not isinstance(cls, type):
      raise TypeError(f'unexpected type {cls}')

    rpc = RpcClient(
      id=self.id,
      queue_name=self.queue_name,
      callback_timeout=self.callback_timeout,
      connection=self.connection,
      channel=self.channel)

    endpoints = self.get_endpoints(cls, rpc)
    if not endpoints:
      raise ValueError(f'no rpc endpoint on class {cls}')

    class_name: str = f'{cls.__name__}Client'
    attributes = { e.name: e.send for e in endpoints }
    attributes['rpc'] = rpc
    Client = type(class_name, (cls,), attributes)
    return Client(*args, **kwargs)


  def get_server(self, cls: Type[T], *args, **kwargs) -> T:
    if not isinstance(cls, type):
      raise TypeError(f'unexpected type {cls}')

    instance = cls(*args, **kwargs)
    endpoints = self.get_endpoints(instance, None)
    if not endpoints:
      raise ValueError(f'no rpc endpoint on class {cls}')

    methods = { e.name: e for e in endpoints }
    rpc = RpcServer(
      id=self.id,
      queue_name=self.queue_name,
      callback_timeout=self.callback_timeout,
      connection=self.connection,
      channel=self.channel,
      endpoints=methods)

    setattr(instance, 'rpc', rpc)
    return instance


def endpoint(func):
  info = Endpoint(func)
  setattr(func, info.attribute, True)
  return func

