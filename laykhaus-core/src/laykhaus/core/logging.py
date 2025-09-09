"""
Structured logging configuration for LaykHaus platform.
"""

import logging
import sys
from typing import Any, Dict, Optional

import structlog
from structlog.contextvars import merge_contextvars
from structlog.processors import (
    CallsiteParameter,
    CallsiteParameterAdder,
    JSONRenderer,
    TimeStamper,
    add_log_level,
)
from structlog.stdlib import ProcessorFormatter, add_logger_name

from laykhaus.core.config import settings


def setup_logging() -> None:
    """Configure structured logging for the application."""
    
    # Configure structlog
    structlog.configure(
        processors=[
            merge_contextvars,
            add_log_level,
            add_logger_name,
            CallsiteParameterAdder(
                parameters=[
                    CallsiteParameter.FILENAME,
                    CallsiteParameter.FUNC_NAME,
                    CallsiteParameter.LINENO,
                ]
            ),
            TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            JSONRenderer() if settings.is_production else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ProcessorFormatter(
            processor=JSONRenderer() if settings.is_production else structlog.dev.ConsoleRenderer(),
            foreign_pre_chain=[
                add_log_level,
                TimeStamper(fmt="iso"),
            ],
        )
    )
    
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, settings.log_level))
    
    # Suppress noisy loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured logger instance
    """
    return structlog.get_logger(name)


class LogContext:
    """Context manager for adding contextual information to logs."""
    
    def __init__(self, **kwargs: Any):
        """Initialize with context variables."""
        self.context = kwargs
        self.tokens: list = []
    
    def __enter__(self) -> "LogContext":
        """Enter context and bind variables."""
        for key, value in self.context.items():
            token = structlog.contextvars.bind_contextvars(**{key: value})
            self.tokens.append(token)
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context and unbind variables."""
        for token in self.tokens:
            structlog.contextvars.unbind_contextvars(token)


def log_performance(operation: str) -> Any:
    """
    Decorator to log function performance.
    
    Args:
        operation: Name of the operation being performed
    """
    import functools
    import time
    from typing import Callable
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = get_logger(func.__module__)
            start_time = time.perf_counter()
            
            try:
                logger.info(f"{operation} started")
                result = await func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                logger.info(
                    f"{operation} completed",
                    duration_ms=round(duration * 1000, 2)
                )
                return result
            except Exception as e:
                duration = time.perf_counter() - start_time
                logger.error(
                    f"{operation} failed",
                    duration_ms=round(duration * 1000, 2),
                    error=str(e)
                )
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = get_logger(func.__module__)
            start_time = time.perf_counter()
            
            try:
                logger.info(f"{operation} started")
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                logger.info(
                    f"{operation} completed",
                    duration_ms=round(duration * 1000, 2)
                )
                return result
            except Exception as e:
                duration = time.perf_counter() - start_time
                logger.error(
                    f"{operation} failed",
                    duration_ms=round(duration * 1000, 2),
                    error=str(e)
                )
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator


# Initialize logging on module import
import asyncio

setup_logging()