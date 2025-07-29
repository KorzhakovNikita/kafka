
class EventError(BaseException):
    pass


class EventValidationError(EventError):
    pass


class EventNotFoundError(EventError):
    pass