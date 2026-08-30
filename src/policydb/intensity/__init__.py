"""Evidence-first policy action and textual intensity measurement.

The public exports stay compatible, but their implementations are loaded on
demand.  Crawl workers import ``policydb.intensity.storage`` for safe Parquet
writes; eagerly importing the package used to pull in the full intensity
service and DuckDB, which created a native 32-thread pool in every worker even
when intensity was disabled.
"""

__all__ = ["HybridDecisionRouter", "PolicyIntensityService"]


def __getattr__(name: str):
    if name == "HybridDecisionRouter":
        from policydb.intensity.router import HybridDecisionRouter

        return HybridDecisionRouter
    if name == "PolicyIntensityService":
        from policydb.intensity.service import PolicyIntensityService

        return PolicyIntensityService
    raise AttributeError(name)
