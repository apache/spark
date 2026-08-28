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

"""Property group classes for property graphs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyspark.graphframes import GraphFrame
from pyspark.sql.functions import col, concat, lit, sha2
from pyspark.sql.types import (
    ByteType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class InvalidPropertyGroupException(Exception):
    """Exception raised when a property group is invalid."""

    pass


class PropertyGroup(ABC):
    """Abstract base class for property groups."""

    def __init__(self, name: str, data: DataFrame) -> None:
        """
        Initialize a property group.

        :param name: The unique identifier for this property group
        :param data: The DataFrame containing the property data
        """
        self._name = name
        self._data = data
        self._validate()

    @property
    def name(self) -> str:
        """Return the name of the property group."""
        return self._name

    @property
    def data(self) -> DataFrame:
        """Return the DataFrame containing the property data."""
        return self._data

    @abstractmethod
    def _validate(self) -> None:
        """Validate the property group. Must be implemented by subclasses."""
        pass

    def get_data(self, filter_col: Column | None = None) -> DataFrame:
        """
        Return a view of the data for the property group.

        :param filter_col: An optional filter condition (Column) to apply to the data
        :return: A DataFrame containing the filtered and optionally transformed data
        """

        if filter_col is None:
            filter_col = lit(True)
        return self._get_data(filter_col)

    @abstractmethod
    def _get_data(self, filter_col: Column) -> DataFrame:
        """Internal method to get filtered data. Must be implemented by subclasses."""
        pass


class VertexPropertyGroup(PropertyGroup):
    """
    Represents a logical group of vertices in a property graph.

    A VertexPropertyGroup organizes and manages vertices that share common characteristics
    or belong to the same logical group within a property graph. Each group maintains its
    own data in the form of a DataFrame and uses a primary key column for unique vertex
    identification.

    When vertices from different groups are combined into a GraphFrame, their IDs are
    hashed with the group name to prevent collisions.

    Example:
        >>> people_data = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> people_group = VertexPropertyGroup("people", people_data, "id")

    :param name: The unique identifier for this vertex property group
    :param data: The DataFrame containing the vertex data
    :param primary_key_column: The column name used to uniquely identify vertices
    :param apply_mask_on_id: Whether to hash IDs with group name (default: True)
    """

    def __init__(
        self,
        name: str,
        data: DataFrame,
        primary_key_column: str = "id",
        apply_mask_on_id: bool = True,
    ) -> None:
        """
        Initialize a VertexPropertyGroup.

        :param name: Name of the vertex property group
        :param data: DataFrame containing vertex data
        :param primary_key_column: Name of the column to use as primary key (default: "id")
        :param apply_mask_on_id: Whether to apply masking on vertex IDs (default: True)
        """
        self._primary_key_column = primary_key_column
        self._apply_mask_on_id = apply_mask_on_id
        super().__init__(name, data)

    @property
    def primary_key_column(self) -> str:
        """Return the primary key column name."""
        return self._primary_key_column

    @property
    def apply_mask_on_id(self) -> bool:
        """Return whether ID masking is applied."""
        return self._apply_mask_on_id

    def _validate(self) -> None:
        """Validate that the primary key column exists in the data."""
        if self._primary_key_column not in self._data.columns:
            raise InvalidPropertyGroupException(
                f"source column {self._primary_key_column} does not exist, "
                f"existed columns [{', '.join(self._data.columns)}]"
            )

    def _get_internal_id_mapping(self) -> DataFrame:
        """
        Create a mapping from external IDs to internal hashed IDs.

        :return: DataFrame with columns 'external_id' and 'id'
        """

        EXTERNAL_ID = "external_id"

        return self._data.select(col(self._primary_key_column).alias(EXTERNAL_ID)).withColumn(
            GraphFrame.ID,
            concat(
                lit(self._name),
                sha2(col(EXTERNAL_ID).cast(StringType()), 256),
            ),
        )

    def _get_data(self, filter_col: Column) -> DataFrame:
        """
        Return filtered vertex data with internal IDs and property group column.

        :param filter_col: Filter condition to apply
        :return: DataFrame with columns 'id' and 'property_group'
        """
        PROPERTY_GROUP_COL_NAME = "property_group"

        filtered_data = self._data.filter(filter_col)

        if self._apply_mask_on_id:
            result = filtered_data.select(
                concat(
                    lit(self._name),
                    sha2(col(self._primary_key_column).cast(StringType()), 256),
                ).alias(GraphFrame.ID)
            )
        else:
            result = filtered_data.select(
                col(self._primary_key_column).cast(StringType()).alias(GraphFrame.ID)
            )

        return result.select(
            col(GraphFrame.ID),
            lit(self._name).alias(PROPERTY_GROUP_COL_NAME),
        )


class EdgePropertyGroup(PropertyGroup):
    """
    Represents a logical group of edges in a property graph.

    EdgePropertyGroup encapsulates edge data stored in a DataFrame along with metadata
    describing how to interpret the data as graph edges. Each edge group has:

    - A unique name identifier
    - DataFrame containing the actual edge data
    - Source and destination vertex property groups
    - Direction flag indicating if edges are directed or undirected
    - Column names specifying source vertex, destination vertex, and edge weight

    When edges from different groups are combined into a GraphFrame, their src and dst
    are hashed with the group name to prevent ID collisions.

    Example:
        >>> edges_data = spark.createDataFrame([(1, 2, 1.0)], ["src", "dst", "weight"])
        >>> edges_group = EdgePropertyGroup(
        ...     "likes", edges_data, people_group, movies_group,
        ...     is_directed=False, src_column="src", dst_column="dst", weight_column="weight"
        ... )

    :param name: Unique identifier for this edge property group
    :param data: DataFrame containing the edge data
    :param src_property_group: Source vertex property group
    :param dst_property_group: Destination vertex property group
    :param is_directed: Whether edges should be treated as directed
    :param src_column_name: Name of the source vertex column in the data
    :param dst_column_name: Name of the destination vertex column in the data
    :param weight_column_name: Name of the edge weight column in the data
    """

    def __init__(
        self,
        name: str,
        data: DataFrame,
        src_property_group: VertexPropertyGroup,
        dst_property_group: VertexPropertyGroup,
        is_directed: bool,
        src_column_name: str,
        dst_column_name: str,
        weight_column_name: str | None = None,
    ) -> None:
        """
        Initialize an EdgePropertyGroup.

        :param name: Unique identifier for this edge property group
        :param data: DataFrame containing the edge data with required columns
        :param src_property_group: Source vertex property group
        :param dst_property_group: Destination vertex property group
        :param is_directed: Whether edges are directed (True) or undirected (False)
        :param src_column_name: Name of the source vertex column
        :param dst_column_name: Name of the destination vertex column
        :param weight_column_name: Name of the edge weight column
                                   (None means the lit(1).alias("weight") will be used)
        """
        if weight_column_name is None:
            data = data.withColumn("weight", lit(1.0))
            weight_column_name = "weight"

        self._src_property_group = src_property_group
        self._dst_property_group = dst_property_group
        self._is_directed = is_directed
        self._src_column_name = src_column_name
        self._dst_column_name = dst_column_name
        self._weight_column_name = weight_column_name
        super().__init__(name, data)

    @property
    def src_property_group(self) -> VertexPropertyGroup:
        """Return the source vertex property group."""
        return self._src_property_group

    @property
    def dst_property_group(self) -> VertexPropertyGroup:
        """Return the destination vertex property group."""
        return self._dst_property_group

    @property
    def is_directed(self) -> bool:
        """Return whether edges are directed."""
        return self._is_directed

    @property
    def src_column_name(self) -> str:
        """Return the source column name."""
        return self._src_column_name

    @property
    def dst_column_name(self) -> str:
        """Return the destination column name."""
        return self._dst_column_name

    @property
    def weight_column_name(self) -> str:
        """Return the weight column name."""
        return self._weight_column_name

    def _validate(self) -> None:
        """Validate that required columns exist and weight column is numeric."""
        if self._src_column_name not in self._data.columns:
            raise InvalidPropertyGroupException(
                f"source column {self._src_column_name} does not exist, "
                f"existed columns [{', '.join(self._data.columns)}]"
            )
        if self._dst_column_name not in self._data.columns:
            raise InvalidPropertyGroupException(
                f"dest column {self._dst_column_name} does not exist, "
                f"existed columns [{', '.join(self._data.columns)}]"
            )
        if self._weight_column_name not in self._data.columns:
            raise InvalidPropertyGroupException(
                f"weight column {self._weight_column_name} does not exist, "
                f"existed columns [{', '.join(self._data.columns)}]"
            )

        # Check weight column type
        weight_column_type = self._data.schema[self._weight_column_name].dataType
        if not self._is_numeric_type(weight_column_type):
            _msg = "weight column {} must be numeric type, but was {}"
            raise InvalidPropertyGroupException(
                _msg.format(self._weight_column_name, weight_column_type)
            )

    def _is_numeric_type(self, data_type: DataType) -> bool:
        """Check if a Spark data type is numeric."""

        numeric_types = (
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
        )
        return isinstance(data_type, numeric_types)

    def _hash_src_edge(self) -> Column:
        """Hash the source edge ID based on the source property group settings."""

        if self._src_property_group.apply_mask_on_id:
            return concat(
                lit(self._src_property_group.name),
                sha2(col(self._src_column_name).cast(StringType()), 256),
            )
        else:
            return col(self._src_column_name).cast(StringType())

    def _hash_dst_edge(self) -> Column:
        """Hash the destination edge ID based on the destination property group settings."""
        if self._dst_property_group.apply_mask_on_id:
            return concat(
                lit(self._dst_property_group.name),
                sha2(col(self._dst_column_name).cast(StringType()), 256),
            )
        else:
            return col(self._dst_column_name).cast(StringType())

    def _get_data(self, filter_col: Column) -> DataFrame:
        """
        Return filtered edge data with hashed IDs and weights.

        For undirected edges, creates bidirectional edges.

        :param filter_col: Filter condition to apply
        :return: DataFrame with columns 'src', 'dst', and 'weight'
        """
        filtered_data = self._data.filter(filter_col)

        base_edges = filtered_data.select(
            self._hash_src_edge().alias(GraphFrame.SRC),
            self._hash_dst_edge().alias(GraphFrame.DST),
            col(self._weight_column_name).alias(GraphFrame.WEIGHT),
        )

        if self._is_directed:
            return base_edges
        else:
            # For undirected edges, create bidirectional edges
            reverse_edges = base_edges.select(
                col(GraphFrame.DST).alias(GraphFrame.SRC),
                col(GraphFrame.SRC).alias(GraphFrame.DST),
                col(GraphFrame.WEIGHT).alias(GraphFrame.WEIGHT),
            )
            return base_edges.union(reverse_edges)
