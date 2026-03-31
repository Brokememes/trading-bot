from __future__ import annotations

import json
import urllib.request
from typing import Protocol

from .models import AlertEvent, TelegramSettings


class Notifier(Protocol):
    def send(self, alert: AlertEvent) -> None: ...


class ConsoleNotifier:
    def send(self, alert: AlertEvent) -> None:
        print(
            f"[ALERT] {alert.timestamp.isoformat()} {alert.pair} {alert.direction} "
            f"futures={alert.raw_futures_price:.6f} spot={alert.spot_price:.6f} "
            f"gap={alert.gap_pips:.2f} pips"
        )


class TelegramNotifier:
    def __init__(self, settings: TelegramSettings) -> None:
        self._enabled = bool(settings.enabled and settings.bot_token and settings.chat_id)
        self._bot_token = settings.bot_token
        self._chat_id = settings.chat_id

    def send(self, alert: AlertEvent) -> None:
        if not self._enabled:
            return

        message = (
            f"{alert.pair} {alert.direction}\n"
            f"futures={alert.raw_futures_price:.6f}\n"
            f"spot={alert.spot_price:.6f}\n"
            f"gap={alert.gap_pips:.2f} pips\n"
            f"time={alert.timestamp.isoformat()}"
        )
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
            data=json.dumps({"chat_id": self._chat_id, "text": message}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10):
            return


class CompositeNotifier:
    def __init__(self, *notifiers: Notifier) -> None:
        self._notifiers = notifiers

    def send(self, alert: AlertEvent) -> None:
        for notifier in self._notifiers:
            notifier.send(alert)
