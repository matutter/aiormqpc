import logging
import uuid
from asyncio.futures import Future
from typing import Any, Awaitable, Callable, Dict, List, Type, get_type_hints

import aiormq
from aiormq.abc import DeliveredMessage
from aiormq.connection import Connection
from pamqp.commands import Basic, Channel, Queue
from pydantic import Field
from pydantic.main import BaseModel

log = logging.getLogger(__name__)

def UUID4Str() -> str:
  return uuid.uuid4().hex


class RpcMessage(BaseModel):
  id: str = Field(default_factory=UUID4Str)
  method: str
  source: str
  type: str
  data: Any


class RpcBase:
  _connection: Connection
  _channel: Channel

  id: str
  queue_name: str
  message_types: Dict[str, Type[BaseModel]]

  def __init__(self, id:str = None, queue_name:str = 'rpc', models: List[Type[BaseModel]] = None) -> None:
    self._connection = None
    self._channel = None
    self.id = id or UUID4Str()
    self.message_types = dict()
    self.queue_name = queue_name
    for model in models:
      self.add_model(model)


  @property
  def is_connected(self) -> bool:
    return bool(self._connection and self._channel)


  def add_model(self, model_type:Type[BaseModel]) -> str:
    type_name: str = model_type.__qualname__
    self.message_types[type_name] = model_type
    return type_name


  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if self.is_connected:
      return
    try:
      if self._connection is None:
        self._connection = await aiormq.connect(dsn)
      self._channel = await self._connection.channel()
    except:
      await self.disconnect()
      raise


  async def disconnect(self) -> None:
    try:
      if self._connection:
        await self._connection.close()
    finally:
      self._connection = None
      self._channel = None


  def process_message(self, msg: RpcMessage) -> None:
    cls: Type[BaseModel] = self.message_types.get(msg.type)
    if msg.data and cls is not None:
      msg.data = cls.parse_obj(msg.data)


class RpcServer(RpcBase):

  methods: Dict[str, Callable[[BaseModel], Awaitable[None]]]

  def __init__(self, id:str = None, queue_name:str = 'rpc', models=None) -> None:
    super().__init__(id, queue_name, models=models)
    self.methods = dict()

  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if self.is_connected:
      return

    await super().connect(dsn=dsn)
    ch: Channel = self._channel
    await ch.queue_declare(queue=self.queue_name, exclusive=True)
    await ch.basic_consume(self.queue_name, self.on_call)


  def add_method(self, callback: Callable[[BaseModel], Awaitable[None]], name=None):
    if name is None:
      name = callback.__name__
    hints: Dict[str, Type] = get_type_hints(callback)
    # TODO Add type checking
    for model in hints.values():
      if issubclass(model, BaseModel):
        self.add_model(model)
    self.methods[name] = callback


  async def on_call(self, message: DeliveredMessage):
    await message.channel.basic_ack(message.delivery.delivery_tag)

    reply_to: str = getattr(message.header.properties, 'reply_to', None)
    msg: RpcMessage = RpcMessage.parse_raw(message.body)

    if msg.method not in self.methods:
      log.warning('Cannot process RPC method %s, method unknown', msg.method)
      return

    self.process_message(msg)

    res = await self.methods[msg.method](msg.data)
    if reply_to is not None:
      await self._send_result(reply_to, msg, res)


  async def _send_result(self, routing_key: str, request: RpcMessage, model: BaseModel) -> None:
    model_type = type(model).__qualname__
    method = request.method + '_result'
    msg = RpcMessage(id=request.id, source=self.id, method=method, type=model_type, data=model)
    data = msg.json().encode()
    props = Basic.Properties(content_type='application/json')
    await self._channel.basic_publish(data, routing_key=routing_key, properties=props)


class RpcClient(RpcBase):

  callback_queue_name: str
  _futures: Dict[str, Future]

  def __init__(self, id:str = None, queue_name:str = 'rpc', models=None) -> None:
    super().__init__(id, queue_name, models=models)
    self._futures = dict()


  async def connect(self, dsn:str = "amqp://guest:guest@localhost/") -> None:
    if self.is_connected:
      return

    await super().connect(dsn=dsn)
    ch: Channel = self._channel
    # Create a unique callback queue for this client
    declare_ok:Queue.DeclareOk = await ch.queue_declare(queue='', exclusive=True)
    self.callback_queue_name = declare_ok.queue
    await ch.basic_consume(self.callback_queue_name, self.on_result)


  async def on_result(self, message: DeliveredMessage):
    await message.channel.basic_ack(message.delivery.delivery_tag)

    msg: RpcMessage = RpcMessage.parse_raw(message.body)

    if msg.id not in self._futures:
      log.warning('Cannot resolve message result %s, unknown message ID', msg.id)
      return

    self.process_message(msg)
    future = self._futures.pop(msg.id)
    future.set_result(msg.data)


  async def call(self, method: str, model: BaseModel):
    model_type = type(model).__qualname__
    msg = RpcMessage(source=self.id, method=method, type=model_type, data=model)
    data = msg.json().encode()
    props = Basic.Properties(
      content_type='application/json',
      reply_to=self.callback_queue_name)

    future = Future()
    self._futures[msg.id] = future
    await self._channel.basic_publish(data, routing_key=self.queue_name, properties=props)
    return await future
