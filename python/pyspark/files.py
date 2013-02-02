import os


class SparkFiles(object):
    """
    Resolves paths to files added through
    L{SparkContext.addFile()<pyspark.context.SparkContext.addFile>}.

    SparkFiles contains only classmethods; users should not create SparkFiles
    instances.
    """

    _root_directory = None
    _is_running_on_worker = False
    _sc = None

    def __init__(self):
        raise NotImplementedError("Do not construct SparkFiles objects")

    @classmethod
    def get(cls, filename):
        """
        Get the absolute path of a file added through C{SparkContext.addFile()}.
        """
        path = os.path.join(SparkFiles.getRootDirectory(), filename)
        return os.path.abspath(path)

    @classmethod
    def getRootDirectory(cls):
        """
        Get the root directory that contains files added through
        C{SparkContext.addFile()}.
        """
        if cls._is_running_on_worker:
            return cls._root_directory
        else:
            # This will have to change if we support multiple SparkContexts:
            return cls._sc._jvm.spark.SparkFiles.getRootDirectory()
