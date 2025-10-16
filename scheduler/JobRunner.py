"""
Job Runner Module

Manages background job scheduling using APScheduler for periodic tasks like
portfolio updates, token analysis, and volume monitoring.
"""

from config.Config import get_config
from logs.logger import get_logger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from config.SchedulerConfig import SCHEDULER_CONFIG
from scheduler.TradingScheduler import TradingScheduler
from scheduler.CredentialResetScheduler import CredentialResetScheduler
from scheduler.PreventShutdownScheduler import PreventShutdownScheduler
import time
import requests
from sqlalchemy.exc import OperationalError, SQLAlchemyError


logger = get_logger(__name__)

# Default retry settings
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds


def with_retries(job_func, scheduler_class):
    """Wrapper for job execution with retry logic."""
    logger.info(f"Starting {job_func.__name__} execution")
    for attempt in range(MAX_RETRIES):
        try:
            scheduler = scheduler_class()
            job_func(scheduler)
            logger.info(f"{job_func.__name__} completed successfully")
            break
        except (
            requests.exceptions.RequestException,
            OperationalError,
            SQLAlchemyError,
            TimeoutError,
            ConnectionError,
        ) as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Retryable error on attempt {attempt + 1}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(
                    f"{job_func.__name__} failed after {MAX_RETRIES} attempts: {e}"
                )
                raise


def run_trading_updates_job():
    """Run trading data updates with retry logic."""
    with_retries(TradingScheduler.handleTradingUpdatesFromJob, TradingScheduler)


def run_credential_reset_job():
    """Run credential reset job with retry logic."""
    with_retries(CredentialResetScheduler.runDailyResetJob, CredentialResetScheduler)


def run_prevent_shutdown_job():
    """Run prevent shutdown job with retry logic."""
    with_retries(PreventShutdownScheduler.handlePreventShutdownFromJob, PreventShutdownScheduler)


class JobRunner:
    """
    Manages APScheduler for scheduling and executing background jobs.

    Features:
    - Configurable job schedules via config
    - Persistent job store with SQLAlchemy
    - Job execution monitoring and logging
    """

    def __init__(self):
        """Initialize scheduler with job store and event listeners."""
        config = get_config()
        self.scheduler = BackgroundScheduler(**SCHEDULER_CONFIG)
        try:
            db_url = config.get_database_url()
            if "jobstores" not in SCHEDULER_CONFIG:
                self.scheduler.add_jobstore(SQLAlchemyJobStore(url=db_url), "default")
                logger.info("Added SQLAlchemy job store")
            self.setup_jobs()
            logger.info("JobRunner initialized")
        except Exception as e:
            logger.error(f"Failed to initialize JobRunner: {e}")
            logger.warning("Using in-memory job store")

    def setup_jobs(self):
        """Configure all scheduled jobs with configurable triggers."""
        config = get_config()
        jobs = [
            ("trading_updates", {"minute": "*/5"}),
            ("credential_reset", {"hour": "*/12", "minute": 0}),
            ("prevent_shutdown", {"second": "*/20"})
        ]
        for job_id, default_schedule in jobs:
            schedule = default_schedule

            # Use named functions instead of lambdas
            if job_id == "trading_updates":
                job_func = run_trading_updates_job
            elif job_id == "credential_reset":
                job_func = run_credential_reset_job
            elif job_id == "prevent_shutdown":
                job_func = run_prevent_shutdown_job
            

            self.scheduler.add_job(
                func=job_func,
                trigger="cron",
                **schedule,
                id=job_id,
                name=job_id.replace("_", " ").title(),
                replace_existing=True,
            )
            logger.info(f"Added job: {job_id}")

    def _job_listener(self, event):
        """Log job execution events."""
        job_id = event.job_id
        if event.exception:
            logger.error(f"Job {job_id} failed: {event.exception}")
        else:
            logger.info(f"Job {job_id} succeeded")


    def start(self):
        """Start the scheduler if not already running."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")
