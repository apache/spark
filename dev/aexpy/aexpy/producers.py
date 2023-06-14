from dataclasses import dataclass
import logging
from abc import ABC
from logging import Logger


@dataclass
class ProducerOptions:
    def load(self, data: "dict"):
        for k, v in data.items():
            setattr(self, k, v)


class Producer(ABC):
    """Producer that produces a product."""

    @classmethod
    def cls(cls) -> "str":
        """Returns the class name of the producer, used by ProducerConfig."""
        return f"{cls.__module__}.{cls.__qualname__}"

    @property
    def name(self) -> "str":
        return self._name if hasattr(self, "_name") else self.cls()

    @name.setter
    def name(self, value: "str") -> None:
        self._name = value

    def __init__(self, logger: "Logger | None" = None) -> None:
        self.logger = (
            logger.getChild(self.name) if logger else logging.getLogger(self.name)
        )
        """The logger for the producer."""
        self.options = ProducerOptions()
        self.name = "empty"
