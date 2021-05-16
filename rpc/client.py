import importlib
import logging
import sys
import uuid
from asyncio.futures import Future
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, Union, get_type_hints

import aiormq
from aiormq.abc import DeliveredMessage
from aiormq.connection import Connection
from pamqp.commands import Basic, Channel, Queue
from pamqp.header import BasicProperties
from pydantic import Field
from pydantic.main import BaseModel

log = logging.getLogger(__name__)

def UUID4Str() -> str:
  return uuid.uuid4().hex


class RpcMessage(BaseModel):
  id: str = Field(default_factory=UUID4Str)
  reply_to: Optional[str]
  method: str
  source: str
  type: str


class RpcBase:
  _connection: Connection
  _channel: Channel

  id: str
  queue_name: str
  message_types: Dict[str, Type[BaseModel]]

  def __init__(self, id:str = None, queue_name:str = 'rpc') -> None:
    self._connection = None
    self._channel = None
    self.id = id or UUID4Str()
    self.message_types = dict()
    self.queue_name = queue_name


  @property
  def is_connected(self) -> bool:
    return bool(self._connection and self._channel)


  def _read_properties(self, message: DeliveredMessage) -> RpcMessage:
    properties: BasicProperties = message.header.properties
    msg = RpcMessage(**properties.headers)
    if not msg.reply_to:
      msg.reply_to = properties.reply_to
    return msg


  def _write_properties(self, msg: RpcMessage, reply_to:str = None) -> BasicProperties:
    headers = msg.dict(exclude={'reply_to'})
    content_type:str = 'application/json'
    if msg.type == 'bytes':
      content_type = 'application/octet-stream'
    props = Basic.Properties(content_type=content_type, headers=headers, reply_to=reply_to)
    return props


  def _get_typename(self, cls: Type[BaseModel]):
    module = cls.__module__
    type_name = cls.__qualname__
    if module == 'builtins':
      return type_name
    return f'{module}.{type_name}'


  def _get_class_by_typename(self, type_name: str) -> Optional[Type[BaseModel]]:
    if type_name in self.message_types:
      return self.message_types[type_name]

    if '.' not in type_name:
      return globals().get(type_name, None)

    cls: Type = None
    module, type_name = type_name.rsplit('.', 1)
    if module not in sys.modules:
      log.debug('Loading module %s for RPC type %s', module, type_name)
      importlib.import_module(module)

    log.debug('Lookup RPC type %s in module %s', type_name, module)
    cls = getattr(sys.modules[module], type_name, None)
    if cls and issubclass(cls, BaseModel):
      self.add_model(cls)
    return cls


  def add_model(self, message_type:Type[BaseModel]) -> str:
    type_name: str = self._get_typename(message_type)
    self.message_types[type_name] = message_type
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


  def encode_message(self, msg: RpcMessage, message:Union[BaseModel, bytes]) -> bytes:
    if msg.type == 'bytes':
      return message
    return message.json().encode()


  def decode_message(self, msg: RpcMessage, message:Union[str, bytes]) -> Union[BaseModel, bytes]:
    if msg.type == 'bytes':
      return message
    cls = self._get_class_by_typename(msg.type)
    return cls.parse_raw(message)


class RpcServer(RpcBase):

  methods: Dict[str, Callable[[BaseModel], Awaitable[None]]]

  def __init__(self, id:str = None, queue_name:str = 'rpc') -> None:
    super().__init__(id, queue_name)
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

    try:
      msg: RpcMessage = self._read_properties(message)

      if msg.method not in self.methods:
        log.warning('Cannot process RPC method %s, method unknown', msg.method)
        return

      data = self.decode_message(msg, message.body)
      result = await self.methods[msg.method](data)
      msg.type = self._get_typename(type(result))
      await self._send_result(msg, result)
    except:
      log.exception('Error handling call')

  async def _send_result(self, msg: RpcMessage, message: Union[BaseModel, bytes]) -> None:
    props = self._write_properties(msg)
    data = self.encode_message(msg, message)
    log.debug('Replying to=%s, method=%s, type=%s', msg.reply_to, msg.method, msg.type)
    await self._channel.basic_publish(data, routing_key=msg.reply_to, properties=props)


class RpcClient(RpcBase):

  callback_queue_name: str
  _futures: Dict[str, Future]

  def __init__(self, id:str = None, queue_name:str = 'rpc') -> None:
    super().__init__(id, queue_name)
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

    msg: RpcMessage = self._read_properties(message)

    if msg.id not in self._futures:
      log.warning('Cannot resolve message result %s, unknown message ID', msg.id)
      return

    future = self._futures.pop(msg.id)
    message = self.decode_message(msg, message.body)
    future.set_result(message)


  async def call(self, method: str, message: Union[BaseModel, bytes]):
    msg = RpcMessage(source=self.id, method=method, type=self._get_typename(type(message)))
    props = self._write_properties(msg, reply_to=self.callback_queue_name)
    data = self.encode_message(msg, message)

    future = Future()
    self._futures[msg.id] = future

    log.debug('Sending queue=%s, id=%s, method=%s', self.queue_name, msg.id, msg.method)
    await self._channel.basic_publish(data, routing_key=self.queue_name, properties=props)
    return await future
