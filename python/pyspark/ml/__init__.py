import inspect

from pyspark import SparkContext

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
