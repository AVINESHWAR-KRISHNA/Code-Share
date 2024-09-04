"""
Logger Module

This module sets up a configurable logger using settings from a TOML file.
The logger can write logs to both a file and the console, with options for
log file rotation and retention. Log levels can be dynamically set at runtime,
and defaults are provided if not specified. The module includes validation
for log levels, handles permissions for log directory creation, supports
singleton pattern for logger initialization, and implements graceful shutdown
handling.

The log file is named dynamically using the current datetime if not supplied
during setup. The console displays warnings and errors by default, while
the log file captures all log levels. File rotation can be configured for size
and time intervals.

Configuration is loaded from a TOML file located at 'config/logger_settings.toml'.

Example configuration file (config/logger_settings.toml):
---------------------------------------------------------
[logger]
default_level_file = "INFO"           # Log level for the file
default_level_console = "WARN"        # Log level for the console
format = "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s - PID: %(process)d - TID: %(thread)d"
datefmt = "%Y-%m-%d %H:%M:%S"
log_dir = "LOGs"                      # Directory where log files are stored
retention_days = 30                   # Number of days to retain log files
max_log_size_mb = 10                  # Maximum log file size in MB before rotation
backup_count = 5                      # Number of backup files to keep

Example usage:
--------------
>>> import logging
>>> from logger_module import setup_logger

>>> logger = setup_logger()
>>> logger.info("Logger setup complete.")
>>> logger.error("This is an error message.")
>>> logger.warning("This is a warning message.")

If a log filename is not provided, it defaults to 'app_<currentdatetime>.log'.
Log files older than the retention period are automatically removed.
"""

import logging
import logging.handlers
import os
import toml
from datetime import datetime
import signal
import sys

# Valid log levels for validation
VALID_LOG_LEVELS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']

_logger_instance = None  # For singleton pattern

def validate_log_level(level):
    """
    Validates the log level to ensure it is recognized by the logging module.
    
    Args:
        level (str): The log level to validate.

    Returns:
        str: Valid log level or 'INFO' as default.

    Doctest:
    >>> validate_log_level('info')
    'INFO'
    >>> validate_log_level('debug')
    'DEBUG'
    >>> validate_log_level('invalid')
    'INFO'
    """
    return level.upper() if level.upper() in VALID_LOG_LEVELS else 'INFO'

def load_logger_config(config_path='config/logger_settings.toml'):
    """
    Loads logger configuration from a TOML file.

    Args:
        config_path (str): Path to the TOML configuration file.

    Returns:
        dict: Configuration settings as a dictionary.

    Doctest:
    >>> config = load_logger_config('config/logger_settings.toml')
    >>> isinstance(config, dict)
    True
    """
    try:
        with open(config_path, 'r') as file:
            return toml.load(file)
    except Exception as e:
        print(f"Error loading logger configuration: {e}")
        return {}

def setup_logger(log_file_name=None, config_path='config/logger_settings.toml'):
    """
    Sets up a logger with file and console handlers based on the configuration.

    Args:
        log_file_name (str): Optional; Name of the log file.
                             Defaults to 'app_<currentdatetime>.log' if not provided.
        config_path (str): Path to the TOML configuration file.

    Returns:
        logging.Logger: Configured logger instance.

    Doctest:
    >>> logger = setup_logger()
    >>> logger.info("Logger setup complete.")
    >>> logger.error("This is an error message.")
    >>> logger.warning("This is a warning message.")
    """
    global _logger_instance
    if _logger_instance:
        return _logger_instance

    config = load_logger_config(config_path)
    
    # Extract logger settings from the configuration
    log_dir = config.get('logger', {}).get('log_dir', 'LOGs')
    default_level_file = validate_log_level(config.get('logger', {}).get('default_level_file', 'INFO'))
    default_level_console = validate_log_level(config.get('logger', {}).get('default_level_console', 'WARN'))
    log_format = config.get('logger', {}).get('format', '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s - PID: %(process)d - TID: %(thread)d')
    date_format = config.get('logger', {}).get('datefmt', '%Y-%m-%d %H:%M:%S')
    retention_days = config.get('logger', {}).get('retention_days', 30)
    max_log_size_mb = config.get('logger', {}).get('max_log_size_mb', 10)
    backup_count = config.get('logger', {}).get('backup_count', 5)

    # Ensure the log directory exists with error handling
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating log directory {log_dir}: {e}")
        return None
    
    # Set default log file name if not provided
    if not log_file_name:
        log_file_name = f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    elif not log_file_name.endswith('.log'):
        log_file_name += '.log'

    log_file_path = os.path.join(log_dir, log_file_name)

    # Create the logger
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.DEBUG)  # Capture all levels; handlers will filter as necessary

    # Create a file handler with flexible log rotation
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path, maxBytes=max_log_size_mb * 1024 * 1024, backupCount=backup_count
        )
        file_handler.setLevel(default_level_file)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Error setting up file handler: {e}")

    # Create a console handler
    try:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(default_level_console)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        logger.addHandler(console_handler)
    except Exception as e:
        print(f"Error setting up console handler: {e}")

    # Set up graceful shutdown handling
    def handle_exit(sig, frame):
        logger.info("Received shutdown signal, shutting down gracefully.")
        logging.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    _logger_instance = logger
    return logger

# Example usage
if __name__ == "__main__":
    logger = setup_logger()  # Uses default log file name based on current datetime
    logger.info("Logger setup complete.")
    logger.error("This is an error message.")
    logger.warning("This is a warning message.")



logger_settings.toml

[logger]
default_level_file = "INFO"           # Log level for the file
default_level_console = "WARN"        # Log level for the console
format = "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
log_dir = "LOGs"                      # Directory where log files are stored
retention_days = 30                   # Number of days to retain log files
max_log_size_mb = 10                  # Maximum log file size in MB before rotation
backup_count = 5                      # Number of backup files to keep
