#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os


__all__ = ["SparkFiles"]

from typing import cast, ClassVar, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark import SparkContext


class SparkFiles:

    """
    Resolves paths to files added through :meth:`SparkContext.addFile`.

    SparkFiles contains only classmethods; users should not create SparkFiles
    instances.
    """

    _root_directory: ClassVar[Optional[str]] = None
    _is_running_on_worker: ClassVar[bool] = False
    _sc: ClassVar[Optional["SparkContext"]] = None

    def __init__(self) -> None:
        raise NotImplementedError("Do not construct SparkFiles objects")

    @classmethod
    def get(cls, filename: str) -> str:
        """
        Get the absolute path of a file added through
        :meth:`SparkContext.addFile` or :meth:`SparkContext.addPyFile`.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        filename : str
            file that are added to resources

        Returns
        -------
        str
            the absolute path of the file

        See Also
        --------
        :meth:`SparkFiles.getRootDirectory`
        :meth:`SparkContext.addFile`
        :meth:`SparkContext.addPyFile`
        :meth:`SparkContext.listFiles`

        Examples
        --------
        >>> import os
        >>> import tempfile
        >>> from pyspark import SparkFiles

        >>> with tempfile.TemporaryDirectory(prefix="get") as d:
        ...     path1 = os.path.join(d, "test.txt")
        ...     with open(path1, "w") as f:
        ...         _ = f.write("100")
        ...
        ...     sc.addFile(path1)
        ...     file_list1 = sorted(sc.listFiles)
        ...
        ...     def func1(iterator):
        ...         path = SparkFiles.get("test.txt")
        ...         assert path.startswith(SparkFiles.getRootDirectory())
        ...         return [path]
        ...
        ...     path_list1 = sc.parallelize([1, 2, 3, 4]).mapPartitions(func1).collect()
        ...
        ...     path2 = os.path.join(d, "test.py")
        ...     with open(path2, "w") as f:
        ...         _ = f.write("import pyspark")
        ...
        ...     # py files
        ...     sc.addPyFile(path2)
        ...     file_list2 = sorted(sc.listFiles)
        ...
        ...     def func2(iterator):
        ...         path = SparkFiles.get("test.py")
        ...         assert path.startswith(SparkFiles.getRootDirectory())
        ...         return [path]
        ...
        ...     path_list2 = sc.parallelize([1, 2, 3, 4]).mapPartitions(func2).collect()
        >>> file_list1
        ['file:/.../test.txt']
        >>> set(path_list1)
        {'.../test.txt'}
        >>> file_list2
        ['file:/.../test.py', 'file:/.../test.txt']
        >>> set(path_list2)
        {'.../test.py'}
        """
        path = os.path.join(SparkFiles.getRootDirectory(), filename)
        return os.path.abspath(path)

    @classmethod
    def getRootDirectory(cls) -> str:
        """
        Get the root directory that contains files added through
        :meth:`SparkContext.addFile` or :meth:`SparkContext.addPyFile`.

        .. versionadded:: 0.7.0

        Returns
        -------
        str
            the root directory that contains files added to resources

        See Also
        --------
        :meth:`SparkFiles.get`
        :meth:`SparkContext.addFile`
        :meth:`SparkContext.addPyFile`

        Examples
        --------
        >>> from pyspark.core.files import SparkFiles
        >>> SparkFiles.getRootDirectory()  # doctest: +SKIP
        '.../spark-a904728e-08d3-400c-a872-cfd82fd6dcd2/userFiles-648cf6d6-bb2c-4f53-82bd-e658aba0c5de'
        """
        if cls._is_running_on_worker:
            return cast(str, cls._root_directory)
        else:
            # This will have to change if we support multiple SparkContexts:
            assert cls._sc is not None
            assert cls._sc._jvm is not None
            return getattr(cls._sc._jvm, "org.apache.spark.SparkFiles").getRootDirectory()


def _test() -> None:
    import doctest
    import sys
    from pyspark import SparkContext

    globs = globals().copy()
    globs["sc"] = SparkContext("local[2]", "files tests")
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
