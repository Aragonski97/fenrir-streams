from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import dotenv_values
from os.path import basename
from pathlib import Path
import smtplib


class MailSender:
    BASE_DIR = Path(__file__).resolve().parent.parent

    env: dict = dotenv_values(dotenv_path=Path(BASE_DIR, ".env"))

    mail_username: str = env.get("MAIL_USERNAME")
    mail_password: str = env.get("MAIL_PASSWORD")

    def send_email(self, mail: MIMEText | MIMEMultipart) -> None:
        server = smtplib.SMTP_SSL(
            host="smtp.gmail.com",
            port=465)
        server.ehlo()
        server.login(
            user=self.mail_username,
            password=self.mail_password)
        server.sendmail(
            msg=mail.as_string(),
            from_addr=self.mail_username,
            to_addrs=mail.get("To"))
        server.close()

    def send_email_without_attachments(
            self,
            text: str,
            subject: str,
            to: list[str] | None = None,
            sender: str | None = None,
            cc: list[str] | None = None,
            bcc: list[str] | None = None) -> None:

        mail = MIMEText(_text=text, _subtype="html")
        mail["Subject"] = subject
        mail["From"] = sender if sender else "BIA Consulting"
        mail["To"] = ", ".join(to) if to else self.mail_username
        if cc:
            mail["Cc"] = ", ".join(cc)
            mail["To"] += mail["Cc"]
        if bcc:
            mail["Bcc"] = ", ".join(bcc)
            mail["To"] += mail["Bcc"]

        self.send_email(mail=mail)

    def send_email_with_attachments(
            self,
            files: list[Path],
            to: list[str],
            subject: str,
            sender: str | None = None,
            text: str | None = None,
            cc: list[str] | None = None,
            bcc: list[str] | None = None) -> None:

        mail: MIMEMultipart = MIMEMultipart()
        mail["Subject"] = subject
        mail["From"] = sender if sender else "BIA Consulting"
        mail["To"] = ", ".join(to)
        if cc:
            mail["Cc"] = ", ".join(cc)
        if bcc:
            mail["Bcc"] = ", ".join(bcc)
        mail.attach(MIMEText(text))

        for file in files:
            with open(file, "rb") as attachment:
                part = MIMEApplication(attachment.read(), Name=basename(attachment))
                part["Content-Disposition"] = f'attachment; filename="{basename(attachment)}"'
            mail.attach(part)

        self.send_email(mail=mail)
