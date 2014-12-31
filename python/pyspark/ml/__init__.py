import inspect

from pyspark import SparkContext
from pyspark.ml.param import Param

__all__ = ["Pipeline"]

# An implementation of PEP3102 for Python 2.
_keyword_only_secret = 70861589


def _assert_keyword_only_args():
    """
    Checks whether the _keyword_only trick is applied and validates input arguments.
    """
    # Get the frame of the function that calls this function.
    frame = inspect.currentframe().f_back
    info = inspect.getargvalues(frame)
    if "_keyword_only" not in info.args:
        raise ValueError("Function does not have argument _keyword_only.")
    if info.locals["_keyword_only"] != _keyword_only_secret:
        raise ValueError("Must use keyword arguments instead of positional ones.")

def _jvm():
    return SparkContext._jvm

class Pipeline(object):

    def __init__(self):
        self.stages = Param(self, "stages", "pipeline stages")
        self.paramMap = {}

    def setStages(self, value):
        self.paramMap[self.stages] = value
        return self

    def getStages(self):
        if self.stages in self.paramMap:
            return self.paramMap[self.stages]

    def fit(self, dataset):
        transformers = []
        for stage in self.getStages():
            if hasattr(stage, "transform"):
                transformers.append(stage)
                dataset = stage.transform(dataset)
            elif hasattr(stage, "fit"):
                model = stage.fit(dataset)
                transformers.append(model)
                dataset = model.transform(dataset)
        return PipelineModel(transformers)


class PipelineModel(object):

    def __init__(self, transformers):
        self.transformers = transformers

    def transform(self, dataset):
        for t in self.transformers:
            dataset = t.transform(dataset)
        return dataset
