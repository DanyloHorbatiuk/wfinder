import httpx

from core.setting import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


def send_telegram_message(text: str, chat_id: str | None = None) -> None:
    settings = get_settings()
    if settings.notify_dry_run:
        logger.info(f"[dry-run] telegram message:\n{text}")
        return
    url = f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage"
    payload = {
        "chat_id": chat_id or settings.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        response = httpx.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("telegram message sent successfully")
    except httpx.HTTPStatusError as e:
        logger.error(f"telegram API error: {e.response.text}")
    except httpx.RequestError as e:
        logger.error(f"telegram network error: {e}")