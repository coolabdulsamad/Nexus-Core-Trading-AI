"""
utils/telegram.py
Send notifications via Telegram bot.

Network on this host is intermittent (DNS / 'Network is unreachable' errors),
so a single failed POST used to drop the alert permanently. We now retry a few
times with backoff and reuse a session. This does NOT fix the underlying WSL
networking problem — if every attempt fails, the box genuinely can't reach
api.telegram.org and you must fix DNS/connectivity there.
"""
import time
import requests
from config.settings import config
from utils.logger import setup_logger

logger = setup_logger("Telegram", "logs/telegram.log")

_MAX_ATTEMPTS = 3
_TIMEOUT = 10          # was 5s; too tight on a slow link
_BACKOFF = 3           # seconds, multiplied by attempt number

_session = requests.Session()


def send_telegram(message):
    if not (config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID):
        logger.info(f"Telegram not configured. Message: {message}")
        return False

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": config.TELEGRAM_CHAT_ID, "text": message}

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = _session.post(url, json=payload, timeout=_TIMEOUT)
            if response.status_code == 200:
                return True
            logger.warning(f"Telegram send failed (attempt {attempt}/{_MAX_ATTEMPTS}): "
                           f"{response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Telegram error (attempt {attempt}/{_MAX_ATTEMPTS}): {e}")
        if attempt < _MAX_ATTEMPTS:
            time.sleep(_BACKOFF * attempt)

    logger.error(f"Telegram: giving up after {_MAX_ATTEMPTS} attempts. Message: {message}")
    return False