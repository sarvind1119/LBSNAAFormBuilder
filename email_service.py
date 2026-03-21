"""
email_service.py - SMTP email sending for document re-upload notifications.
Uses Python stdlib smtplib - no extra dependencies needed.
"""

import smtplib
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from html import escape

logger = logging.getLogger(__name__)


def get_smtp_config():
    """Read SMTP configuration from environment variables."""
    return {
        'host': os.environ.get('SMTP_HOST', ''),
        'port': int(os.environ.get('SMTP_PORT', '587')),
        'username': os.environ.get('SMTP_USERNAME', ''),
        'password': os.environ.get('SMTP_PASSWORD', ''),
        'use_tls': os.environ.get('SMTP_USE_TLS', 'true').lower() == 'true',
        'from_email': os.environ.get('SMTP_FROM_EMAIL', 'noreply@lbsnaa.gov.in'),
        'from_name': os.environ.get('SMTP_FROM_NAME', 'LBSNAA Form Builder'),
    }


def is_configured():
    """Check if SMTP is configured (host is set)."""
    config = get_smtp_config()
    return bool(config['host'])


def send_notification_email(to_email, course_name, submission_id, doc_label,
                            reason, admin_message, deadline, reupload_url):
    """
    Send a notification email about a flagged document with a re-upload link.
    Returns True on success, raises Exception on failure.
    """
    config = get_smtp_config()

    if not config['host']:
        raise RuntimeError("SMTP not configured. Set SMTP_HOST environment variable.")

    subject = f"[{course_name}] Document Re-upload Required - Submission #{submission_id}"

    # Build HTML email
    html_body = _build_html_email(
        course_name=escape(course_name),
        submission_id=submission_id,
        doc_label=escape(doc_label),
        reason=escape(reason),
        admin_message=escape(admin_message),
        deadline=escape(deadline),
        reupload_url=reupload_url,
    )

    # Build plain text fallback
    text_body = _build_text_email(
        course_name=course_name,
        submission_id=submission_id,
        doc_label=doc_label,
        reason=reason,
        admin_message=admin_message,
        deadline=deadline,
        reupload_url=reupload_url,
    )

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f"{config['from_name']} <{config['from_email']}>"
    msg['To'] = to_email

    msg.attach(MIMEText(text_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP(config['host'], config['port'], timeout=30) as server:
        if config['use_tls']:
            server.starttls()
        if config['username']:
            server.login(config['username'], config['password'])
        server.send_message(msg)

    logger.info(f"Sent notification email to {to_email} for submission #{submission_id}")
    return True


def _build_html_email(course_name, submission_id, doc_label, reason,
                      admin_message, deadline, reupload_url):
    """Build the HTML email body."""
    admin_msg_html = ""
    if admin_message:
        admin_msg_html = f"""
        <tr>
            <td style="padding: 20px 30px;">
                <p style="margin: 0 0 8px; font-weight: 600; color: #374151;">Message from Admin:</p>
                <div style="background: #f0fdf4; border-left: 4px solid #16a34a; padding: 12px 16px; border-radius: 4px;">
                    <p style="margin: 0; color: #374151;">{admin_message}</p>
                </div>
            </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin: 0; padding: 0; background-color: #f3f4f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f3f4f6; padding: 40px 0;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background: white; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow: hidden;">
                    <!-- Header -->
                    <tr>
                        <td style="background: #166534; padding: 24px 30px;">
                            <h1 style="margin: 0; color: white; font-size: 20px; font-weight: 600;">LBSNAA Form Builder</h1>
                        </td>
                    </tr>
                    <!-- Title -->
                    <tr>
                        <td style="padding: 30px 30px 10px;">
                            <h2 style="margin: 0; color: #dc2626; font-size: 18px;">Document Re-upload Required</h2>
                        </td>
                    </tr>
                    <!-- Course Info -->
                    <tr>
                        <td style="padding: 10px 30px;">
                            <table width="100%" style="background: #f9fafb; border-radius: 6px; padding: 16px;">
                                <tr>
                                    <td style="padding: 4px 16px; color: #6b7280; font-size: 14px;">Course</td>
                                    <td style="padding: 4px 16px; font-weight: 600; color: #111827;">{course_name}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 4px 16px; color: #6b7280; font-size: 14px;">Submission ID</td>
                                    <td style="padding: 4px 16px; font-weight: 600; color: #111827;">#{submission_id}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 4px 16px; color: #6b7280; font-size: 14px;">Flagged Document</td>
                                    <td style="padding: 4px 16px; font-weight: 600; color: #dc2626;">{doc_label}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 4px 16px; color: #6b7280; font-size: 14px;">Reason</td>
                                    <td style="padding: 4px 16px; color: #374151;">{reason}</td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    <!-- Admin Message -->
                    {admin_msg_html}
                    <!-- Deadline -->
                    <tr>
                        <td style="padding: 20px 30px;">
                            <p style="margin: 0; color: #374151;">
                                Please re-upload the corrected document before
                                <strong style="color: #dc2626;">{deadline}</strong>.
                            </p>
                        </td>
                    </tr>
                    <!-- CTA Button -->
                    <tr>
                        <td style="padding: 10px 30px 30px;" align="center">
                            <a href="{reupload_url}"
                               style="display: inline-block; background: #166534; color: white; padding: 14px 32px;
                                      border-radius: 6px; text-decoration: none; font-weight: 600; font-size: 16px;">
                                Re-upload Document
                            </a>
                        </td>
                    </tr>
                    <!-- Footer -->
                    <tr>
                        <td style="background: #f9fafb; padding: 20px 30px; border-top: 1px solid #e5e7eb;">
                            <p style="margin: 0; color: #9ca3af; font-size: 12px;">
                                This is an automated message from LBSNAA Form Builder.
                                The re-upload link is single-use and will expire on {deadline}.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""


def _build_text_email(course_name, submission_id, doc_label, reason,
                      admin_message, deadline, reupload_url):
    """Build the plain text email body."""
    lines = [
        "DOCUMENT RE-UPLOAD REQUIRED",
        "=" * 40,
        "",
        f"Course: {course_name}",
        f"Submission ID: #{submission_id}",
        f"Flagged Document: {doc_label}",
        f"Reason: {reason}",
        "",
    ]
    if admin_message:
        lines.extend([
            "Message from Admin:",
            admin_message,
            "",
        ])
    lines.extend([
        f"Please re-upload the corrected document before {deadline}.",
        "",
        f"Re-upload link: {reupload_url}",
        "",
        "This link is single-use and will expire on the deadline above.",
        "",
        "---",
        "LBSNAA Form Builder",
    ])
    return "\n".join(lines)
