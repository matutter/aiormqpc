import asyncio
import logging
from asyncio.futures import Future
from inspect import FullArgSpec, getfullargspec
from typing import (Any, Awaitable, Callable, Dict, List, Optional, Type,
                    TypeVar)
from uuid import uuid1

import aiormq
import msgpack
from aiormq.abc import DeliveredMessage
from aiormq.connection import Connection
from pamqp.commands import Channel, Queue
from pydantic import BaseModel
from pydantic.fields import Field

T = TypeVar('T')

log = logging.getLogger(__name__)


class RpcErrorMessage(BaseModel):
  """
  Used to capture server-side errors so they may be returned to the client.
  """

  msg: str
  error_class: str


class RpcMessage(BaseModel):
  """
  The standard message send for each RPC call and reply.
  """

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

  error: Optional[RpcErrorMessage]

  @classmethod
  def from_exception(cls, e: Exception, orig: 'RpcMessage'):
    error = RpcErrorMessage(msg=str(e), error_class=type(e).__name__)
    if not orig:
      return cls(name='Error', error=error)
    return cls(id=orig.id, name=orig.name, error=error)


class RpcError(Exception):
  """
  An error thrown client-side after a server-side error has been captured and
  sent back to the client.
  """

  send: RpcMessage
  reply: RpcMessage

  def __init__(self, send: RpcMessage = None, reply: RpcMessage = None) -> None:
    self.send = send
    self.reply = reply
    source: str = 'rpc call'
    if send and send.name:
      source = send.name
    if reply and reply.error:
      e: RpcErrorMessage = reply.error
      msg = f'Remote RPC error: {e.error_class} {e.msg} (from {source})'
    else:
      msg = f'Unexpected RPC error while contacting {source}'
    super().__init__(msg)


class RpcProvider:
  """
  Base class for both RpcClient and RpcServer classes.
  """

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
          id: str = None,
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

  async def connect(self, dsn: str = "amqp://guest:guest@localhost/") -> None:
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

  """
  The RpcServer acts as a dispatcher between the RPC subscriptions and the
  actual RPC method. It will automatically call RPC server-side Endpoints
  and send return values back to the client.
  """

  async def connect(self, dsn: str = "amqp://guest:guest@localhost/") -> None:
    if not self.is_connected:
      await super().connect(dsn=dsn)

    await self.channel.queue_declare(queue=self.queue_name, exclusive=True)
    await self.channel.basic_consume(self.queue_name, self.receive)

  async def receive(self, message: DeliveredMessage) -> None:
    msg = RpcMessage.parse_obj(msgpack.unpackb(message.body))
    endpoint = self.endpoints[msg.name]
    response: RpcMessage
    try:
      response = await endpoint.receive(msg)
    except Exception as e:
      log.exception(f'Failed to receive RPC message on %s', endpoint)
      response = RpcMessage.from_exception(e, orig=msg)

    data = msgpack.packb(response.dict())
    await self.channel.basic_publish(data, routing_key=msg.reply_to)


class RpcClient(RpcProvider):
  """
  The RpcClient class is used by the RpcFactory to generate client-side RPC
  interfaces in the form a facade class.
  """

  async def connect(self, dsn: str = "amqp://guest:guest@localhost/") -> None:
    if not self.is_connected:
      await super().connect(dsn=dsn)
    declare_ok: Queue.DeclareOk = await self.channel.queue_declare(queue='', exclusive=True)
    self.callback_queue_name = declare_ok.queue
    await self.channel.basic_consume(self.callback_queue_name, self.receive)

  async def send(self, msg: RpcMessage) -> RpcMessage:
    msg.reply_to = self.callback_queue_name
    data = msgpack.packb(msg.dict())
    future = Future()
    self.pending_results[msg.id] = future
    await self.channel.basic_publish(data, routing_key=self.queue_name)
    reply: RpcMessage = await asyncio.wait_for(future, self.callback_timeout)
    if reply.error:
      log.warning('rpc error from %s %s', msg.name, reply.error)
      raise RpcError(send=msg, reply=reply)

    return reply

  async def receive(self, message: DeliveredMessage):
    msg = RpcMessage.parse_obj(msgpack.unpackb(message.body))
    future = self.pending_results[msg.id]
    future.set_result(msg)


class Endpoint:
  """
  The Endpoint class wraps each individual RPC method and is responsible for
  (de)serialization from the transport format.
  """

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
    """
    Preparses an endpoints type hints so that methods may be (de)serialized with
    less run-time type inspection.
    """

    self.provider = provider
    self.func = func
    self.name = getattr(func, '__name__', str(func))
    self.argspec: FullArgSpec = getfullargspec(func)
    self.return_type = None
    self.arg_models = dict()
    self.keyword_models = dict()

    annotations = dict(self.argspec.annotations)
    args = [a for a in self.argspec.args if a != 'self']

    for key, val in annotations.items():
      if not self._check_type_support(val):
        raise TypeError(f'unexpected type {val} for {key} of {self.name}')

    if 'return' in annotations:
      self.return_type = annotations.pop('return')

    self.keyword_models = {
        k: v for k, v in annotations.items() if issubclass(v, BaseModel)}

    for i, arg in enumerate(args):
      if arg not in annotations:
        log.warning(
            f'missing type annotation for argument {arg} of {self.name}')
        continue
      if issubclass(annotations[arg], BaseModel):
        self.arg_models[i] = annotations.pop(arg)

  def _check_type_support(self, type_: Type):
    return issubclass(type_, (BaseModel, str, int, float, str, bytes, bool))

  async def send(self, *args: Any, **kwargs: Any) -> Any:
    """
    Invoked on client side endpoints. All `args` and `kwargs` are converted to
    an RpcMessage and send to the server-side endpoints `receive` message.

    The server response is handled and returned to caller by this method.

    A timeout error may be thrown if the server does not response within the
    `callback_timeout`.
    """
    args = list(args)
    for i in self.arg_models.keys():
      if i >= len(args):
        break
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
    """
    Invokes on server side Endpoints when a message is received. Exception
    thrown from within this method are converted to RpcErrorMessages and return
    to the client.
    """
    args = list(msg.args)
    for i, cls in self.arg_models.items():
      if i >= len(args):
        break
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
    if not obj:
      return False
    return getattr(obj, cls.attribute, False)

  def __repr__(self) -> str:
    return f'Endpoint({self.name})'


class RpcFactory(RpcProvider):

  """
  A factory generator for RPC endpoints backed by RabbitMQ.

  If `connect` is called on the factory then RpcClients and RpcServers created
  by this factory will inherit the AIORMQ connection and channel.
  """

  def get_endpoints(self, cls: Type, provider: RpcProvider) -> List[Endpoint]:
    """
    Return a list Endpoints for each method decorated by `rpc.endpoint` on the
    class `cls`.
    """

    endpoints = list()
    for attr in dir(cls):
      func = getattr(cls, attr)
      if not Endpoint.has_endpoint(func):
        continue
      end = Endpoint(func, provider)
      endpoints.append(end)
    return endpoints

  def get_client(self, cls: Type[T], *args, **kwargs) -> T:
    """
    Create a facade instance of `cls` where methods decorated with the
    `rpc.endpoint` decorator will send RpcMessage's to remote endpoint.

    Only methods of `cls` decorated with `rpc.endpoint` will be present on the
    facade class returned by this method.
    """

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
    attributes = {e.name: e.send for e in endpoints}
    attributes['rpc'] = rpc
    Client = type(class_name, (cls,), attributes)
    return Client(*args, **kwargs)

  def get_server(self, cls: Type[T], *args, **kwargs) -> T:
    """
    Create a new instance of `cls` backed by an RpcServer. The instance's
    methods will be invoked automatically by the RpcServer. The server is
    accessible by the `rpc` attribute assigned to the instance of `cls`.

    Only methods of `cls` decorated with `rpc.endpoint` will be visible to
    RpcClients.
    """

    if not isinstance(cls, type):
      raise TypeError(f'unexpected type {cls}')

    instance = cls(*args, **kwargs)
    endpoints = self.get_endpoints(instance, None)
    if not endpoints:
      raise ValueError(f'no rpc endpoint on class {cls}')

    methods = {e.name: e for e in endpoints}
    rpc = RpcServer(
        id=self.id,
        queue_name=self.queue_name,
        callback_timeout=self.callback_timeout,
        connection=self.connection,
        channel=self.channel,
        endpoints=methods)

    setattr(instance, 'rpc', rpc)
    return instance


def endpoint(func: Callable[[Any], Awaitable]):
  """
  Decorator for RPC endpoints - only methods using this decorator will be
  accessible by RpcClients.
  """

  info = Endpoint(func)
  setattr(func, info.attribute, True)
  return func
