from abc import ABCMeta, abstractmethod
from typing import Set, Dict, Sequence

from duckdq.core.preconditions import Precondition
from duckdq.core.metrics import Metric
from duckdq.metadata.metadata_repository import MetadataRepository
from duckdq.core.properties import Property


class Engine(metaclass=ABCMeta):
    @abstractmethod
    def profile(self) -> Dict:
        pass

    @abstractmethod
    def evaluate_preconditions(self, preconditions: Sequence[Precondition]):
        pass

    @abstractmethod
    def compute_metrics(self, properties: Set[Property], repo: MetadataRepository) -> Dict[Property, Metric]:
        pass

