

class SerializerError(BaseException):
    def __init__(self, detail: str = None):
        self.detail = detail or "Serializer error occurred"
        super().__init__(self.detail)


class DictSerializerError(SerializerError):
    def __init__(self, detail: str = None):
        self.detail = detail or "Failed to serialize dictionary"
        super().__init__(self.detail)


class JsonSerializerError(SerializerError):
    def __init__(self, detail: str = None):
        self.detail = detail or "Failed to serialize JSON"
        super().__init__(self.detail)


class StringSerializerError(SerializerError):
    def __init__(self, detail: str = None):
        self.detail = detail or "Failed to serialize string"
        super().__init__(self.detail)
