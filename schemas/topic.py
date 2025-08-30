from pydantic import BaseModel


class CreateNewTopic(BaseModel):
    name: str
    num_partitions: int
    replication_factor: int


