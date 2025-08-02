from datetime import date
from enum import Enum
from typing import Union

from pydantic import BaseModel, HttpUrl, Field


class EmailType(str, Enum):
    ACCOUNT_CREATED = 'account_created'
    CONFIRMATION = 'confirmation_code'
    NEW_PROMOCODE = 'new_promocode'


class AccountCreatedSchema(BaseModel):
    username: str
    created_at: str
    login_url: HttpUrl


class ConfirmationCodeSchema(BaseModel):
    code: str
    expires_in: int


class PromoCodeSchema(BaseModel):
    promo_code: str
    discount_amount: int = Field(ge=0, le=100)
    expiry_date: date


class SendEmailEventSchema(BaseModel):
    email_type: EmailType
    template_params: Union[AccountCreatedSchema, ConfirmationCodeSchema, PromoCodeSchema]

