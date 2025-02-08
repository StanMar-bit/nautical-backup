#!/usr/bin/env python3
import os
import time
import shutil
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def remove_old_backups(retention_count=None, retention_days=None, dry_run=False):
    """
    Purge old backups from each container's directory under /destination.
    
    Each container's backups are assumed to be stored as subdirectories within /destination.
    
    Args:
        retention_count (int or None): Maximum number of backups to keep per container.
        retention_days (int or None): Maximum age (in days) for backups.
        dry_run (bool): If True, only log what would be deleted without performing deletion.
    """
    destination_dir = "/destination"

    if not os.path.exists(destination_dir):
        logger.error("Destination directory '%s' does not exist.", destination_dir)
        return

    # Process each container (each subdirectory in /destination)
    for container in os.listdir(destination_dir):
        container_path = os.path.join(destination_dir, container)
        if not os.path.isdir(container_path):
            continue  # Skip non-directory items

        backups = []
        # Assume each backup is a subdirectory within the container directory.
        for backup in os.listdir(container_path):
            backup_path = os.path.join(container_path, backup)
            if os.path.isdir(backup_path):
                try:
                    mtime = os.path.getmtime(backup_path)
                except Exception as e:
                    logger.error("Could not get modification time for %s: %s", backup_path, e)
                    continue
                backups.append({'name': backup, 'path': backup_path, 'time': mtime})

        if not backups:
            logger.info("No backups found for container '%s'.", container)
            continue

        # Sort backups by modification time (oldest first)
        backups.sort(key=lambda b: b['time'])
        backups_to_delete = set()

        # Retention by count: delete the oldest backups if exceeding the allowed count.
        if retention_count is not None and len(backups) > retention_count:
            excess = len(backups) - retention_count
            for b in backups[:excess]:
                backups_to_delete.add(b['path'])
                logger.debug("Marking backup '%s' for deletion by count policy.", b['name'])

        # Retention by age: delete backups older than the allowed age.
        if retention_days is not None:
            cutoff_time = time.time() - (retention_days * 86400)  # 86400 seconds per day
            for b in backups:
                if b['time'] < cutoff_time:
                    backups_to_delete.add(b['path'])
                    logger.debug("Marking backup '%s' for deletion by age policy.", b['name'])

        # Delete (or log) the backups marked for deletion.
        for b in backups:
            if b['path'] in backups_to_delete:
                backup_date = datetime.fromtimestamp(b['time']).strftime("%Y-%m-%d %H:%M:%S")
                if dry_run:
                    logger.info("[DRY RUN] Would remove backup '%s' (created %s) for container '%s'.",
                                b['name'], backup_date, container)
                else:
                    logger.info("Removing backup '%s' (created %s) for container '%s'.",
                                b['name'], backup_date, container)
                    try:
                        shutil.rmtree(b['path'])
                    except Exception as e:
                        logger.error("Error deleting backup '%s' for container '%s': %s",
                                     b['name'], container, e)


def main():
    # Read retention parameters from environment variables.
    retention_count_env = os.environ.get("BACKUP_RETENTION_COUNT")
    retention_days_env = os.environ.get("BACKUP_RETENTION_DAYS")
    dry_run_env = os.environ.get("DRY_RUN", "False")

    try:
        retention_count = int(retention_count_env) if retention_count_env is not None else None
    except ValueError:
        logger.error("Invalid BACKUP_RETENTION_COUNT value: %s. It should be an integer.", retention_count_env)
        retention_count = None

    try:
        retention_days = int(retention_days_env) if retention_days_env is not None else None
    except ValueError:
        logger.error("Invalid BACKUP_RETENTION_DAYS value: %s. It should be an integer.", retention_days_env)
        retention_days = None

    dry_run = dry_run_env.lower() in ["true", "1", "yes"]

    logger.info(
        "Starting backup retention cleanup in '/destination' with retention_count=%s, retention_days=%s, dry_run=%s",
        retention_count, retention_days, dry_run
    )

    remove_old_backups(retention_count=retention_count, retention_days=retention_days, dry_run=dry_run)


if __name__ == "__main__":
    main()
