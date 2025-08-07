from events.base_event import AbstractEvent
from infrastructure.service_container import AbstractServiceContainer
from schemas.events import SendEmailEventSchema
from utils.template_manager import template_manager


class AccountCreatedHandler:
    template = "account_created.html"
    subject = "Добро пожаловать!"

    async def handle(self, services: AbstractServiceContainer, data: SendEmailEventSchema) -> None:
        html = template_manager.get_template(self.template).render(
            subject=self.subject,
            username=data.template_params.username,
            created_at=data.template_params.created_at,
            login_url=data.template_params.login_url,
        )
        await services.notifications.send(html, self.subject)


class ConfirmationCodeHandler:
    template = "confirmation_code.html"
    subject = "Код подтверждения"

    async def handle(self, services: AbstractServiceContainer, data: SendEmailEventSchema) -> None:
        html = template_manager.get_template(self.template).render(
            subject=self.subject,
            code=data.template_params.code,
            expires_in=data.template_params.expires_in
        )
        await services.notifications.send(html, self.subject)


class PromoCodeHandler:
    template = "promocode.html"
    subject = "Ваш персональный промокод!"

    async def handle(self, services: AbstractServiceContainer, data: SendEmailEventSchema) -> None:
        html = template_manager.get_template(self.template).render(
            subject=self.subject,
            promo_code=data.template_params.promo_code,
            discount_amount=data.template_params.discount_amount,
            expiry_date=data.template_params.expiry_date
        )
        await services.notifications.send(html, self.subject)


class SendEmailEvent(AbstractEvent):
    event_name = 'email-event'
    validation_model = SendEmailEventSchema

    _handlers = {
        'account_created': AccountCreatedHandler,
        'confirmation_code': ConfirmationCodeHandler,
        'new_promocode': PromoCodeHandler,
    }

    async def _on_event(self, data: SendEmailEventSchema):
        handler = self._handlers[data.email_type]()
        await handler.handle(
            services=self._services,
            data=data
        )
