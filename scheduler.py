"""
Scheduler for automatic reforecast updates.
"""
import schedule
import time
import threading
import logging
from datetime import datetime
from typing import Callable, Optional

from config import REFORECAST_TIMES_UTC, REFRESH_INTERVAL_MINUTES

logger = logging.getLogger(__name__)


class ReforecastScheduler:
    def __init__(self, update_callback: Callable):
        self.update_callback = update_callback
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.last_run: Optional[datetime] = None

    def setup_schedule(self):
        for time_str in REFORECAST_TIMES_UTC:
            schedule.every().day.at(time_str).do(self._run_update)
            logger.info(f"Scheduled reforecast update at {time_str} UTC")
        schedule.every(REFRESH_INTERVAL_MINUTES).minutes.do(self._run_update)

    def _run_update(self):
        try:
            logger.info(f"Running scheduled update at {datetime.utcnow()}")
            self.update_callback()
            self.last_run = datetime.utcnow()
        except Exception as e:
            logger.error(f"Scheduled update failed: {e}", exc_info=True)

    def start(self):
        if self._running:
            return
        self.setup_schedule()
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started.")

    def _loop(self):
        while self._running:
            schedule.run_pending()
            time.sleep(30)

    def stop(self):
        self._running = False
        schedule.clear()
        logger.info("Scheduler stopped.")

    def trigger_manual_update(self):
        self._run_update()
