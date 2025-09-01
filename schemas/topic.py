from pydantic import BaseModel, Field


class CreateNewTopic(BaseModel):
    name: str
    num_partitions: int = Field(ge=1)
    replication_factor: int = Field(ge=1)


