import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib
from fastapi import HTTPException
from starlette import status

from infrastructure.notifications import AbstractNotificationsService
from settings import settings


class NotificationsService(AbstractNotificationsService):

    async def send(self, html, subject: str = "KAFKA"):
        msg = MIMEMultipart()
        msg['From'] = settings.SMTP_USERNAME
        msg['To'] = settings.SMTP_USERNAME # your recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(html, 'html'))

        async with aiosmtplib.SMTP(
                hostname=settings.SMTP_HOST,
                port=settings.SMTP_PORT,
        ) as server:
            try:
                await server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
                await server.send_message(msg)
            except (smtplib.SMTPDataError, smtplib.SMTPConnectError) as smtp_err:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"EMAIL_ERROR: {str(smtp_err)}"
                )
