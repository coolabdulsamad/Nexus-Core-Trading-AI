"""Critical phone alarms + dead-man's switch (v3.7.6).

Two channels, both optional and both safe no-ops when not configured:

1. Pushover (https://pushover.net) - the RINGING alarm. priority=2
   (emergency) makes the phone ring repeatedly (every 60s for 10 min) and
   bypass quiet hours until you acknowledge it. The app is a one-time ~$5
   purchase. Keys go in .env (never in code):
       PUSHOVER_APP_TOKEN=...
       PUSHOVER_USER_KEY=...

2. Healthchecks.io (https://healthchecks.io) - the DEAD-MAN'S SWITCH. The
   bot pings a URL every cycle. If the pings STOP (laptop dead, power out,
   internet down, process hung), healthchecks notices from the CLOUD and
   alerts your phone (their app, email, SMS/voice integrations - it also
   connects to Pushover so even this alarm rings). Free tier is enough.
   Put the ping URL in .env:
       HEALTHCHECK_PING_URL=https://hc-ping.com/your-uuid-here

Nothing in this module may ever raise - alerting must never crash trading.
"""

import logging
import os

import requests

logger = logging.getLogger("Alerter")


def send_critical_alert(title: str, body: str = "") -> bool:
    """Ring the phone for critical events: daily profit target / loss limit
    hit, watchdog restarts, broker rejecting closes, runaway positions.

    Pushover emergency priority (2) requires retry + expire: the alert
    re-fires every 60s for 10 minutes until acknowledged on the phone."""
    token = os.getenv("PUSHOVER_APP_TOKEN", "").strip()
    user = os.getenv("PUSHOVER_USER_KEY", "").strip()
    if not token or not user:
        return False
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": token,
                "user": user,
                "title": title[:250],
                "message": (body or title)[:1024],
                "priority": 2,
                "retry": 60,      # seconds between re-alerts until acknowledged
                "expire": 600,    # stop re-alerting after 10 minutes
                "sound": "siren",
            },
            timeout=10,
        )
        if r.status_code != 200:
            logger.error(f"pushover alert rejected: HTTP {r.status_code} {r.text[:200]}")
            return False
        logger.info(f"critical alert sent to phone: {title}")
        return True
    except Exception as e:
        logger.error(f"pushover alert failed: {e}")
        return False


def ping_healthcheck(suffix: str = ""):
    """Dead-man's switch ping. If this stops arriving (machine dead, no
    internet, process hung), healthchecks.io alerts the phone from the cloud.
    A failed ping is silently ignored - the MISSING ping is the alarm."""
    url = os.getenv("HEALTHCHECK_PING_URL", "").strip()
    if not url:
        return
    try:
        requests.get(url + suffix, timeout=5)
    except Exception:
        pass
