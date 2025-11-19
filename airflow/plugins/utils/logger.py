import logging
import inspect

def get_logger() -> logging.Logger:
    """ Returns a module-level logger with the specified logging level. """

    caller_frame = inspect.stack()[1]
    module = inspect.getmodule(caller_frame[0])
    module_name = module.__name__ if module else "nyc_trips_pipeline"

    retunr logging.getLogger(module_name)


def log(msg: str, level: str = "INFO", **kwargs):
    """Log a message with the specified level and additional context. """
    
    logger = get_logger()

    if kwargs:
        msg = f"{msg} | " + " | ".join(f"{k}={v}" for k, v in kwargs.items())

    level = level.upper()
    if level == "DEBUG":
        logger.debug(msg)
    elif level == "WARNING":
        logger.warning(msg)
    elif level == "ERROR":
        logger.error(msg)
    else :
        logger.info(msg)
