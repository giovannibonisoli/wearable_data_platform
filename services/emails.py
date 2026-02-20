from config import EMAIL_SENDER, EMAIL_PASSWORD, SMTP_SERVER, SMTP_PORT

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import smtplib


def send_email(recipient_email, subject, html, text):
    """Invia un email"""

    try:
        # Create the message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_SENDER
        msg['To'] = recipient_email

        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(html, 'html')

        msg.attach(part1)
        msg.attach(part2)

        # Invia l'email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipient_email, msg.as_string())
        server.quit()

        return True
    except Exception as e:
        print(f"Error sending the email: {e}")
        return False