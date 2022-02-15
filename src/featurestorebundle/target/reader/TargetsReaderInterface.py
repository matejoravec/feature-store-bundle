from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class TargetsReaderInterface(ABC):
    @abstractmethod
    def read(self, entity: str) -> DataFrame:
        pass