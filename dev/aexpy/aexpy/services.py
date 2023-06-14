from aexpy.models import (
    ApiDescription,
    ApiDifference,
    Distribution,
    ProduceState,
    Report,
    ProduceMode,
)
from aexpy.caching import ProduceCache
from .extracting import Extractor
from .diffing import Differ
from .reporting import Reporter


class ServiceProvider:
    """Container of all producers, single processing entry point with cache support."""

    def extract(
        self,
        cache: "ProduceCache",
        dist: "Distribution",
        mode: "ProduceMode" = ProduceMode.Access,
        product: "ApiDescription | None" = None,
    ) -> "ApiDescription":
        from .extracting.default import DefaultExtractor

        extractor = DefaultExtractor()
        assert isinstance(extractor, Extractor)
        assert dist.release is not None
        product = product or ApiDescription(distribution=dist)
        with product.produce(cache, mode, extractor.logger) as product:
            if product.state == ProduceState.Pending:
                extractor.extract(dist, product)
            product.producer = extractor.name
        return product

    def diff(
        self,
        cache: "ProduceCache",
        old: "ApiDescription",
        new: "ApiDescription",
        mode: "ProduceMode" = ProduceMode.Access,
        product: "ApiDifference | None" = None,
    ) -> "ApiDifference":
        from .diffing.default import DefaultDiffer

        differ = DefaultDiffer()
        assert isinstance(differ, Differ)
        assert old.distribution is not None and old.distribution.release is not None
        assert new.distribution is not None and new.distribution.release is not None
        product = product or ApiDifference(old=old.distribution, new=new.distribution)
        with product.produce(cache, mode, differ.logger) as product:
            if product.state == ProduceState.Pending:
                differ.diff(old, new, product)
            product.producer = differ.name
        return product

    def report(
        self,
        cache: "ProduceCache",
        diff: "ApiDifference",
        mode: "ProduceMode" = ProduceMode.Access,
        product: "Report | None" = None,
    ) -> "Report":
        from .reporting.text import TextReporter

        reporter = TextReporter()
        assert isinstance(reporter, Reporter)
        product = product or Report(old=diff.old.release, new=diff.new.release)
        with product.produce(cache, mode, reporter.logger) as product:
            if product.state == ProduceState.Pending:
                reporter.report(diff, product)
            product.producer = reporter.name
        return product
