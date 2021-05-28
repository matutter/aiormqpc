# aiormqpc

A very simple RPC interface using [aiormq](https://github.com/mosquito/aiormq)
and [pydantic](https://github.com/samuelcolvin/pydantic).

## Usage

`pydantic` is used to model complex objects which are transparently serialized
and packed into messages.

```python
# Define Pydantic models
class FileData(BaseModel):
  filename: str
  data: bytes
```

Define a class using the `@endpoint` decorator to specify which methods will be
accessible over the rpc interface.

```python
from aiormqpc import RpcProvider, endpoint

# Define an RPC class
class Dropbox:
  rpc: RpcProvider

  files: Dict[str, FileData]
  max_files: int

  def __init__(self, max_files: int = 1000):
    self.files = dict()
    self.max_files = max_files

  @endpoint
  async def upload_file(self, file: FileData) -> int:
    if len(self.files) >= self.max_files:
      # Errors are propagated to the client-side
      raise Exception('too many files')
    self.files[file.name] = file
    return len(file.data)

  @endpoint
  async def download_file(self, name: str) -> FileData:
    return self.files[name]
```

Use `RpcFactory` to create an instance of your server-side rpc class.

```python
factory = RpcFactory()
server = factory.get_server(Dropbox, max_files=2)
# await for the connection and the rpc server will start listening in current event loop
await server.rpc.connect(dsn="amqp://guest:guest@localhost/")
```

The `RpcFactory` dynamically creates the client-side interface, only `endpoint`
decorated methods are available on the client.

```python
factory = RpcFactory()
client = factory.get_client(Dropbox)
await client.rpc.connect(dsn="amqp://guest:guest@localhost/")
```

Invoke methods on the client as if it were an instance of the class.

```python
file1 = FileData(name='file1', data=b'1234')
size = await client.upload_file(file1)
```
