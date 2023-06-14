from ..models import (
    ApiDifference,
    Report,
)
from ..producers import Producer


class Reporter(Producer):
    def report(self, diff: "ApiDifference", product: "Report"):
        """Report the differences between two versions of the API."""
        assert diff.old and diff.new
