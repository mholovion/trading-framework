import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional


# ANSI escape codes
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_DIM    = "\033[2m"

# Level colors
_LEVEL_STYLE = {
    'DEBUG':    "\033[90m",       # dark gray
    'INFO':     "\033[0m",        # default
    'WARNING':  "\033[33m",       # yellow
    'ERROR':    "\033[31m",       # red
    'CRITICAL': "\033[1;31m",     # bold red
}

# Per-service accent colors (applied to the service tag)
_SERVICE_COLOR = {
    'orchestrator':    "\033[36m",    # cyan
    'historical':      "\033[34m",    # blue
    'realtime':        "\033[94m",    # bright blue
    'aggregation':     "\033[35m",    # magenta
    'indicators':      "\033[96m",    # bright cyan
    'indicators_gap':  "\033[96m",
    'strategies':      "\033[32m",    # green
    'strategies_gap':  "\033[92m",    # bright green
    'database':        "\033[37m",    # light gray
    'api':             "\033[33m",    # yellow
    'config':          "\033[90m",    # dark gray
}

_DEFAULT_SERVICE_COLOR = "\033[90m"

# Level symbols
_LEVEL_SYMBOL = {
    'DEBUG':    '·',
    'INFO':     '▸',
    'WARNING':  '⚠',
    'ERROR':    '✗',
    'CRITICAL': '✗',
}

# Service short names (max 5 chars for alignment)
_SERVICE_SHORT = {
    'orchestrator':    'ORCH',
    'historical':      'HIST',
    'realtime':        'LIVE',
    'aggregation':     'AGG',
    'indicators':      'IND',
    'indicators_gap':  'IGAP',
    'strategies':      'STRA',
    'strategies_gap':  'SGAP',
    'database':        'DB',
    'api':             'API',
    'config':          'CONF',
}


class ColorFormatter(logging.Formatter):
    """Compact, colored log formatter for terminal/Docker output."""

    _use_color: bool = True

    def format(self, record: logging.LogRecord) -> str:
        level = record.levelname
        name  = record.name

        ts     = self.formatTime(record, '%H:%M:%S')
        symbol = _LEVEL_SYMBOL.get(level, '▸')
        short  = _SERVICE_SHORT.get(name, name[:5].upper()).ljust(4)
        msg    = record.getMessage()

        if record.exc_info:
            exc = self.formatException(record.exc_info)
            msg = f"{msg}\n{exc}"

        if not self._use_color:
            return f"{ts} │ {short} │ {symbol} {msg}"

        svc_color = _SERVICE_COLOR.get(name, _DEFAULT_SERVICE_COLOR)
        lvl_color = _LEVEL_STYLE.get(level, '')

        ts_str  = f"{_DIM}{ts}{_RESET}"
        sep     = f"{_DIM}│{_RESET}"
        svc_str = f"{_BOLD}{svc_color}{short}{_RESET}"
        sym_str = f"{lvl_color}{symbol}{_RESET}"
        msg_str = f"{lvl_color}{msg}{_RESET}"

        return f"{ts_str} {sep} {svc_str} {sep} {sym_str} {msg_str}"

    def formatTime(self, record, datefmt=None):
        import datetime
        ct = datetime.datetime.fromtimestamp(record.created)
        return ct.strftime(datefmt or '%H:%M:%S')


def _is_tty() -> bool:
    """Return True when stdout looks like a color-capable terminal."""
    if os.getenv('NO_COLOR'):
        return False
    if os.getenv('FORCE_COLOR'):
        return True
    return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()


def _make_formatter() -> logging.Formatter:
    fmt = ColorFormatter()
    # Docker captures stdout — always use color unless explicitly disabled
    fmt._use_color = not bool(os.getenv('NO_COLOR'))
    return fmt


class ServiceLogger:

    @staticmethod
    def setup_logger(service_name: str,
                     log_level: str = 'INFO',
                     log_dir: Optional[str] = None) -> logging.Logger:
        logger = logging.getLogger(service_name)
        logger.handlers.clear()
        logger.propagate = False

        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)

        fmt = _make_formatter()

        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        console.setFormatter(fmt)
        logger.addHandler(console)

        if log_dir:
            log_path = Path(log_dir) / f"{service_name}.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.handlers.RotatingFileHandler(
                filename=log_path,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8',
            )
            fh.setLevel(level)
            # Plain formatter for file (no ANSI)
            plain = ColorFormatter()
            plain._use_color = False
            fh.setFormatter(plain)
            logger.addHandler(fh)

        return logger

    @staticmethod
    def get_log_config() -> dict:
        return {
            'level':        os.getenv('LOG_LEVEL', 'INFO'),
            'base_dir':     os.getenv('LOG_DIR', '/app/logs'),
            'console_only': os.getenv('LOG_CONSOLE_ONLY', 'false').lower() == 'true',
        }


def setup_service_logging(service_name: str) -> logging.Logger:
    config = ServiceLogger.get_log_config()
    log_dir = None if config['console_only'] else config['base_dir']
    return ServiceLogger.setup_logger(
        service_name=service_name,
        log_level=config['level'],
        log_dir=log_dir,
    )


# Predefined loggers
def get_orchestrator_logger()   -> logging.Logger: return setup_service_logging('orchestrator')
def get_database_logger()       -> logging.Logger: return setup_service_logging('database')
def get_historical_logger()     -> logging.Logger: return setup_service_logging('historical')
def get_realtime_logger()       -> logging.Logger: return setup_service_logging('realtime')
def get_indicators_logger()     -> logging.Logger: return setup_service_logging('indicators')
def get_strategies_logger()     -> logging.Logger: return setup_service_logging('strategies')
def get_strategies_gap_logger() -> logging.Logger: return setup_service_logging('strategies_gap')
def get_api_logger()            -> logging.Logger: return setup_service_logging('api')
