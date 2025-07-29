import json
from abc import ABC, abstractmethod
from typing import Any, Type
from pydantic import BaseModel

from exceptions.serializer import DictSerializerError, JsonSerializerError, StringSerializerError


class BaseSerializer(ABC):

    @abstractmethod
    def serialize(self, value: Any) -> bytes: ...


class DictSerializer(BaseSerializer):

    def serialize(self, value):
        try:
            value = json.dumps(value).encode("utf-8")
            return value
        except Exception:
            raise DictSerializerError


class JsonSerializer(BaseSerializer):

    def serialize(self, value):
        try:
            value = value.json().encode("utf-8")
            return value
        except Exception:
            raise JsonSerializerError


class StringSerializer(BaseSerializer):

    def serialize(self, value):
        try:
            value = str(value).encode("utf-8")
            return value
        except Exception:
            raise StringSerializerError


class SerializerRegister:

    def __init__(self):
        self._serializers: dict[Type, BaseSerializer] = {
            str: StringSerializer(),
            dict: DictSerializer(),
            BaseModel: JsonSerializer()
        }
        self._default_serializers = DictSerializer()

    def get_serializer(self, value: Any):

        for serializer_type, serializer in self._serializers.items():
            if isinstance(value, serializer_type):
                return serializer

        return self._default_serializers
