"""
LaykHaus Federation Engine

The core query federation system that enables unified access to heterogeneous data sources.
"""

from laykhaus.federation.spark_federated_executor import SparkFederatedExecutor, FederationResult

__all__ = [
    "SparkFederatedExecutor",
    "FederationResult",
]