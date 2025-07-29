from enum import Enum
from typing import Union

from pydantic import BaseModel, HttpUrl


class EmailType(str, Enum):
    ACCOUNT_CREATED = 'account_created'
    CONFIRMATION = 'confirmation_code'
    CUSTOM_LINK = 'custom_link'


class AccountCreatedSchema(BaseModel):
    username: str
    created_at: str
    login_url: HttpUrl


class ConfirmationCodeSchema(BaseModel):
    code: str
    expires_in: int


class SendEmailEventSchema(BaseModel):
    email_type: EmailType
    template_params: Union[AccountCreatedSchema, ConfirmationCodeSchema]

