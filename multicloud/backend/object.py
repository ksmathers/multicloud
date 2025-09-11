from io import IOBase, BufferedReader, TextIOWrapper, TextIOBase

class Object:
    def __init__(self, ctx, key:str):
        """Gets and puts objectstore value identified by key

        Args:
          ctx :Context:
          key :str: an object key has pathlike syntax (a/b/c), but must not start with a '/'
        """
        from ..autocontext import Context
        assert(not key.startswith('/'))
        self.ctx : Context = ctx
        self.key = key

    def prepare(self, fullpath):
        raise NotImplementedError("base class")

    def put_bytes(self, data : bytes):
        raise NotImplementedError("base class")

    def put_file(self, binary:bool = True) -> IOBase:
        """Opens an output stream that when written to will upload the data to the object store."""
        raise NotImplementedError("base class")

    def get_bytes(self) -> bytes:
        raise NotImplementedError("base class")

    def get_file(self, binary:bool = True) -> IOBase:
        """Opens an input stream that when read from will download the data from the object store."""
        raise NotImplementedError("base class")

    def get_text(self) -> str:
        return self.get_bytes().decode()

    def put_text(self, value:str):
        self.put_bytes(value.encode())

    def get_textfile(self) -> TextIOBase:
        iob = self.get_file(binary=False)
        assert(isinstance(iob, TextIOBase))
        return iob

    def exists(self) -> bool:
        raise NotImplementedError("base class")
