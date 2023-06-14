from ..models import ApiDescription, Distribution
from ..producers import Producer


class Extractor(Producer):
    def extract(self, dist: "Distribution", product: "ApiDescription"):
        """Extract an API description from a distribution."""
        pass
