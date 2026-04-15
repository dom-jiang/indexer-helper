"""
SMTP email utility for sending verification codes.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from loguru import logger
from config import Cfg


def send_verification_code(to_email: str, code: str) -> bool:
    """Send a 6-digit verification code to the given email address."""
    subject = "Your Verification Code - API Boss"
    html = f"""
    <div style="max-width:480px;margin:0 auto;font-family:Arial,sans-serif;color:#333;">
      <div style="background:linear-gradient(135deg,#667eea,#764ba2);padding:24px;border-radius:8px 8px 0 0;">
        <h2 style="color:#fff;margin:0;text-align:center;">API Boss</h2>
      </div>
      <div style="background:#fff;padding:32px;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 8px 8px;">
        <p style="font-size:15px;">Your verification code is:</p>
        <div style="text-align:center;margin:24px 0;">
          <span style="font-size:32px;font-weight:700;letter-spacing:8px;color:#667eea;">{code}</span>
        </div>
        <p style="font-size:13px;color:#999;">This code will expire in 5 minutes. If you did not request this, please ignore this email.</p>
      </div>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = Cfg.SMTP_FROM
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        if int(Cfg.SMTP_PORT) == 465:
            server = smtplib.SMTP_SSL(Cfg.SMTP_HOST, int(Cfg.SMTP_PORT), timeout=10)
        else:
            server = smtplib.SMTP(Cfg.SMTP_HOST, int(Cfg.SMTP_PORT), timeout=10)
            server.starttls()
        server.login(Cfg.SMTP_USER, Cfg.SMTP_PASS)
        server.sendmail(Cfg.SMTP_FROM, [to_email], msg.as_string())
        server.quit()
        logger.info(f"Verification code sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Failed to send verification email to {to_email}: {e}")
        return False
