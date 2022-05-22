from enum import Enum


class ReportFormat(Enum):
    text = 'text'
    json = 'json'

    def __str__(self) -> str:
        return self.value


class LogLevel(Enum):
    debug = 'debug'
    info = 'info'

    def __str__(self):
        return self.value
