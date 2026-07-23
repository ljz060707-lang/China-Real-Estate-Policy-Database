__all__ = ["CrawlPipeline"]


def __getattr__(name: str):
    if name == "CrawlPipeline":
        from policydb.crawl.pipeline import CrawlPipeline

        return CrawlPipeline
    raise AttributeError(name)
