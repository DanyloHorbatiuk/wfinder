"""Task failure alerts (SPEC §7.2). Wired as on_failure_callback via default_args."""
import html
from typing import Any

from core.setting import get_settings
from notify.telegram import send_telegram_message
from utils.logger import get_logger

logger = get_logger(__name__)

EXCEPTION_PREVIEW_LEN = 300


def send_alert(text: str) -> None:
    settings = get_settings()
    chat_id = settings.telegram_alert_chat_id or settings.telegram_chat_id
    send_telegram_message(text, chat_id=chat_id)


def task_failure_alert(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback. Never raises: any error here is caught and logged."""
    try:
        task_instance = context.get("task_instance") or context.get("ti")
        dag_id = html.escape(str(getattr(task_instance, "dag_id", "?")))
        task_id = html.escape(str(getattr(task_instance, "task_id", "?")))
        run_id = html.escape(str(context.get("run_id", "?")))
        try_number = html.escape(str(getattr(task_instance, "try_number", "?")))

        exception = context.get("exception")
        exc_text = html.escape(str(exception)[:EXCEPTION_PREVIEW_LEN]) if exception else "?"

        lines = [
            "❌ Збій задачі",
            f"DAG: {dag_id}",
            f"Задача: {task_id}",
            f"Запуск: {run_id}",
            f"Спроба: {try_number}",
            f"Помилка: {exc_text}",
        ]

        log_url = None
        if task_instance is not None:
            try:
                log_url = task_instance.log_url
            except Exception:
                log_url = None
        if log_url:
            lines.append(f"Лог: {html.escape(str(log_url))}")

        send_alert("\n".join(lines))
    except Exception:
        logger.exception("failed to send task failure alert")
