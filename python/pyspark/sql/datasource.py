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
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Tuple, Union, TYPE_CHECKING

from pyspark.sql import Row
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql._typing import OptionalPrimitiveType


__all__ = ["DataSource", "DataSourceReader"]


class DataSource(ABC):
    """
    A base class for data sources.

    This class represents a custom data source that allows for reading from and/or
    writing to it. The data source provides methods to create readers and writers
    for reading and writing data, respectively. At least one of the methods ``reader``
    or ``writer`` must be implemented by any subclass to make the data source either
    readable or writable (or both).

    After implementing this interface, you can start to load your data source using
    ``spark.read.format(...).load()`` and save data using ``df.write.format(...).save()``.
    """

    def __init__(self, options: Dict[str, "OptionalPrimitiveType"]):
        """
        Initializes the data source with user-provided options.

        Parameters
        ----------
        options : dict
            A dictionary representing the options for this data source.

        Notes
        -----
        This method should not contain any non-serializable objects.
        """
        self.options = options

    @property
    def name(self) -> str:
        """
        Returns a string represents the short name of this data source.
        """
        return self.__class__.__name__

    def schema(self) -> Union[StructType, str]:
        """
        Returns the schema of the data source.

        It can reference the ``options`` field to infer the data source's schema when
        users do not explicitly specify it. This method is invoked once when calling
        ``spark.read.format(...).load()`` to get the schema for a data source read
        operation. If this method is not implemented, and a user does not provide a
        schema when reading the data source, an exception will be thrown.

        Returns
        -------
        schema : StructType or str
            The schema of this data source or a DDL string represents the schema

        Examples
        --------
        Returns a DDL string:

        >>> def schema(self):
        ...    return "a INT, b STRING"

        Returns a StructType:

        >>> def schema(self):
        ...   return StructType().add("a", "int").add("b", "string")
        """
        ...

    def reader(self, schema: StructType) -> "DataSourceReader":
        """
        Returns a DataSourceReader instance for reading data.

        This method is required for readable data sources. It will be called once during
        the physical planning stage in the Spark planner.

        Parameters
        ----------
        schema : StructType
            The schema of the data to be read.

        Returns
        -------
        reader : DataSourceReader
            A reader instance for this data source.
        """
        raise NotImplementedError


class DataSourceReader(ABC):
    """
    A base class for data source readers. Data source readers are responsible for
    outputting data from a data source.
    """

    def partitions(self) -> Iterator[Any]:
        """
        Returns a list of partitions for this data source.

        This method is called once during the physical planning stage to generate a list
        of partitions. If the method returns N partitions, then the planner will create
        N tasks. Each task will then execute ``read(partition)`` in parallel, using each
        partition value to read the data from this data source.

        If this method is not implemented, or returns an empty list, Spark will create one
        partition for the result DataFrame and use one single task to read the data.

        Returns
        -------
        partitions : list
            A list of partitions for this data source. The partition can be any arbitrary
            serializable objects.

        Notes
        -----
        This method should not return any un-picklable objects.

        Examples
        --------
        Returns a list of integers:

        >>> def partitions(self):
        ...     return [1, 2, 3]

        Returns a list of string:

        >>> def partitions(self):
        ...     return ["a", "b", "c"]

        Returns a list of tuples:

        >>> def partitions(self):
        ...     return [("a", 1), ("b", 2), ("c", 3)]
        """
        ...

    @abstractmethod
    def read(self, partition: Any) -> Iterator[Union[Tuple, Row]]:
        """
        Generates data for a given partition and returns an iterator of tuples or rows.

        This method is invoked once per partition by Spark tasks to read the data.
        You can initialize any non-serializable resources required for reading data from
        the data source within this method.

        Implementing this method is required for readable data sources.

        Parameters
        ----------
        partition : object
            The partition to read. It must be one of the partition values returned by
            ``partitions()`` or None if ``partitions()`` method is not implemented.

        Returns
        -------
        iterator : Iterator[Tuple] or Iterator[Row]
            An iterator of tuples or rows. Each tuple or row will be converted to a row
            in the final DataFrame.
        """
        ...
