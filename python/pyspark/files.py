import os


class SparkFiles(object):
    """
    Resolves paths to files added through
    L{addFile()<pyspark.context.SparkContext.addFile>}.

    SparkFiles contains only classmethods; users should not create SparkFiles
    instances.
    """

    _root_directory = None

    def __init__(self):
        raise NotImplementedError("Do not construct SparkFiles objects")

    @classmethod
    def get(cls, filename):
        """
        Get the absolute path of a file added through C{addFile()}.
        """
        path = os.path.join(SparkFiles._root_directory, filename)
        return os.path.abspath(path)
