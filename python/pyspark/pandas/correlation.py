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

from typing import List

from pyspark.sql import DataFrame as SparkDataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.pandas.utils import verify_temp_column_name, is_ansi_mode_enabled


CORRELATION_VALUE_1_COLUMN = "__correlation_value_1_input__"
CORRELATION_VALUE_2_COLUMN = "__correlation_value_2_input__"
CORRELATION_CORR_OUTPUT_COLUMN = "__correlation_corr_output__"
CORRELATION_COUNT_OUTPUT_COLUMN = "__correlation_count_output__"


def compute(sdf: SparkDataFrame, groupKeys: List[str], method: str) -> SparkDataFrame:
    """
    Compute correlation per group, excluding NA/null values.

    Input PySpark Dataframe should contain column `CORRELATION_VALUE_1_COLUMN` and
    column `CORRELATION_VALUE_2_COLUMN`, as well as the group columns.

    The returned PySpark Dataframe will contain the correlation column
    `CORRELATION_CORR_OUTPUT_COLUMN` and the non-null count column
    `CORRELATION_COUNT_OUTPUT_COLUMN`, as well as the group columns.
    """
    assert len(groupKeys) > 0
    assert method in ["pearson", "spearman", "kendall"]

    sdf = sdf.select(
        *[F.col(key) for key in groupKeys],
        *[
            # assign both columns nulls, if some of them are null
            F.when(
                F.isnull(CORRELATION_VALUE_1_COLUMN) | F.isnull(CORRELATION_VALUE_2_COLUMN),
                F.lit(None),
            )
            .otherwise(F.col(CORRELATION_VALUE_1_COLUMN))
            .alias(CORRELATION_VALUE_1_COLUMN),
            F.when(
                F.isnull(CORRELATION_VALUE_1_COLUMN) | F.isnull(CORRELATION_VALUE_2_COLUMN),
                F.lit(None),
            )
            .otherwise(F.col(CORRELATION_VALUE_2_COLUMN))
            .alias(CORRELATION_VALUE_2_COLUMN),
        ],
    )
    spark_session = sdf.sparkSession

    if method in ["pearson", "spearman"]:
        # convert values to avg ranks for spearman correlation
        if method == "spearman":
            ROW_NUMBER_COLUMN = verify_temp_column_name(
                sdf, "__correlation_spearman_row_number_temp_column__"
            )
            DENSE_RANK_COLUMN = verify_temp_column_name(
                sdf, "__correlation_spearman_dense_rank_temp_column__"
            )
            window = Window.partitionBy(groupKeys)

            # CORRELATION_VALUE_1_COLUMN: value -> avg rank
            # for example:
            # values:       3, 4, 5, 7, 7, 7, 9, 9, 10
            # avg ranks:    1.0, 2.0, 3.0, 5.0, 5.0, 5.0, 7.5, 7.5, 9.0
            sdf = (
                sdf.withColumn(
                    ROW_NUMBER_COLUMN,
                    F.row_number().over(
                        window.orderBy(F.asc_nulls_last(CORRELATION_VALUE_1_COLUMN))
                    ),
                )
                # drop nulls but make sure each group contains at least one row
                .where(~F.isnull(CORRELATION_VALUE_1_COLUMN) | (F.col(ROW_NUMBER_COLUMN) == 1))
                .withColumn(
                    DENSE_RANK_COLUMN,
                    F.dense_rank().over(
                        window.orderBy(F.asc_nulls_last(CORRELATION_VALUE_1_COLUMN))
                    ),
                )
                .withColumn(
                    CORRELATION_VALUE_1_COLUMN,
                    F.when(F.isnull(CORRELATION_VALUE_1_COLUMN), F.lit(None)).otherwise(
                        F.avg(ROW_NUMBER_COLUMN).over(
                            window.orderBy(F.asc(DENSE_RANK_COLUMN)).rangeBetween(0, 0)
                        )
                    ),
                )
            )

            # CORRELATION_VALUE_2_COLUMN: value -> avg rank
            sdf = (
                sdf.withColumn(
                    ROW_NUMBER_COLUMN,
                    F.row_number().over(
                        window.orderBy(F.asc_nulls_last(CORRELATION_VALUE_2_COLUMN))
                    ),
                )
                .withColumn(
                    DENSE_RANK_COLUMN,
                    F.dense_rank().over(
                        window.orderBy(F.asc_nulls_last(CORRELATION_VALUE_2_COLUMN))
                    ),
                )
                .withColumn(
                    CORRELATION_VALUE_2_COLUMN,
                    F.when(F.isnull(CORRELATION_VALUE_2_COLUMN), F.lit(None)).otherwise(
                        F.avg(ROW_NUMBER_COLUMN).over(
                            window.orderBy(F.asc(DENSE_RANK_COLUMN)).rangeBetween(0, 0)
                        )
                    ),
                )
            )

        if is_ansi_mode_enabled(spark_session):
            corr_expr = F.try_divide(
                F.covar_samp(CORRELATION_VALUE_1_COLUMN, CORRELATION_VALUE_2_COLUMN),
                F.stddev_samp(CORRELATION_VALUE_1_COLUMN)
                * F.stddev_samp(CORRELATION_VALUE_2_COLUMN),
            )
        else:
            corr_expr = F.corr(CORRELATION_VALUE_1_COLUMN, CORRELATION_VALUE_2_COLUMN)

        sdf = sdf.groupby(groupKeys).agg(
            corr_expr.alias(CORRELATION_CORR_OUTPUT_COLUMN),
            F.count(F.when(~F.isnull(CORRELATION_VALUE_1_COLUMN), 1)).alias(
                CORRELATION_COUNT_OUTPUT_COLUMN
            ),
        )

        return sdf

    else:
        # kendall correlation
        ROW_NUMBER_1_2_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_row_number_1_2_temp_column__"
        )
        sdf = sdf.withColumn(
            ROW_NUMBER_1_2_COLUMN,
            F.row_number().over(
                Window.partitionBy(groupKeys).orderBy(
                    F.asc_nulls_last(CORRELATION_VALUE_1_COLUMN),
                    F.asc_nulls_last(CORRELATION_VALUE_2_COLUMN),
                )
            ),
        )

        # drop nulls but make sure each group contains at least one row
        sdf = sdf.where(~F.isnull(CORRELATION_VALUE_1_COLUMN) | (F.col(ROW_NUMBER_1_2_COLUMN) == 1))

        CORRELATION_VALUE_X_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_value_x_temp_column__"
        )
        CORRELATION_VALUE_Y_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_value_y_temp_column__"
        )
        ROW_NUMBER_X_Y_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_row_number_x_y_temp_column__"
        )
        sdf2 = sdf.select(
            *[F.col(key) for key in groupKeys],
            *[
                F.col(CORRELATION_VALUE_1_COLUMN).alias(CORRELATION_VALUE_X_COLUMN),
                F.col(CORRELATION_VALUE_2_COLUMN).alias(CORRELATION_VALUE_Y_COLUMN),
                F.col(ROW_NUMBER_1_2_COLUMN).alias(ROW_NUMBER_X_Y_COLUMN),
            ],
        )

        sdf = sdf.join(sdf2, groupKeys, "inner").where(
            F.col(ROW_NUMBER_1_2_COLUMN) <= F.col(ROW_NUMBER_X_Y_COLUMN)
        )

        # compute P, Q, T, U in tau_b = (P - Q) / sqrt((P + Q + T) * (P + Q + U))
        # see https://github.com/scipy/scipy/blob/v1.9.1/scipy/stats/_stats_py.py#L5015-L5222
        CORRELATION_KENDALL_P_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_tau_b_p_temp_column__"
        )
        CORRELATION_KENDALL_Q_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_tau_b_q_temp_column__"
        )
        CORRELATION_KENDALL_T_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_tau_b_t_temp_column__"
        )
        CORRELATION_KENDALL_U_COLUMN = verify_temp_column_name(
            sdf, "__correlation_kendall_tau_b_u_temp_column__"
        )

        pair_cond = ~F.isnull(CORRELATION_VALUE_1_COLUMN) & (
            F.col(ROW_NUMBER_1_2_COLUMN) < F.col(ROW_NUMBER_X_Y_COLUMN)
        )

        p_cond = (
            (F.col(CORRELATION_VALUE_1_COLUMN) < F.col(CORRELATION_VALUE_X_COLUMN))
            & (F.col(CORRELATION_VALUE_2_COLUMN) < F.col(CORRELATION_VALUE_Y_COLUMN))
        ) | (
            (F.col(CORRELATION_VALUE_1_COLUMN) > F.col(CORRELATION_VALUE_X_COLUMN))
            & (F.col(CORRELATION_VALUE_2_COLUMN) > F.col(CORRELATION_VALUE_Y_COLUMN))
        )
        q_cond = (
            (F.col(CORRELATION_VALUE_1_COLUMN) < F.col(CORRELATION_VALUE_X_COLUMN))
            & (F.col(CORRELATION_VALUE_2_COLUMN) > F.col(CORRELATION_VALUE_Y_COLUMN))
        ) | (
            (F.col(CORRELATION_VALUE_1_COLUMN) > F.col(CORRELATION_VALUE_X_COLUMN))
            & (F.col(CORRELATION_VALUE_2_COLUMN) < F.col(CORRELATION_VALUE_Y_COLUMN))
        )
        t_cond = (F.col(CORRELATION_VALUE_1_COLUMN) == F.col(CORRELATION_VALUE_X_COLUMN)) & (
            F.col(CORRELATION_VALUE_2_COLUMN) != F.col(CORRELATION_VALUE_Y_COLUMN)
        )
        u_cond = (F.col(CORRELATION_VALUE_1_COLUMN) != F.col(CORRELATION_VALUE_X_COLUMN)) & (
            F.col(CORRELATION_VALUE_2_COLUMN) == F.col(CORRELATION_VALUE_Y_COLUMN)
        )

        if is_ansi_mode_enabled(spark_session):
            corr_expr = F.try_divide(
                F.col(CORRELATION_KENDALL_P_COLUMN) - F.col(CORRELATION_KENDALL_Q_COLUMN),
                F.sqrt(
                    (
                        F.col(CORRELATION_KENDALL_P_COLUMN)
                        + F.col(CORRELATION_KENDALL_Q_COLUMN)
                        + F.col(CORRELATION_KENDALL_T_COLUMN)
                    )
                    * (
                        F.col(CORRELATION_KENDALL_P_COLUMN)
                        + F.col(CORRELATION_KENDALL_Q_COLUMN)
                        + F.col(CORRELATION_KENDALL_U_COLUMN)
                    )
                ),
            )
        else:
            corr_expr = (
                F.col(CORRELATION_KENDALL_P_COLUMN) - F.col(CORRELATION_KENDALL_Q_COLUMN)
            ) / F.sqrt(
                (
                    (
                        F.col(CORRELATION_KENDALL_P_COLUMN)
                        + F.col(CORRELATION_KENDALL_Q_COLUMN)
                        + (F.col(CORRELATION_KENDALL_T_COLUMN))
                    )
                )
                * (
                    (
                        F.col(CORRELATION_KENDALL_P_COLUMN)
                        + F.col(CORRELATION_KENDALL_Q_COLUMN)
                        + (F.col(CORRELATION_KENDALL_U_COLUMN))
                    )
                )
            )

        sdf = (
            sdf.groupby(groupKeys)
            .agg(
                F.count(F.when(pair_cond & p_cond, 1)).alias(CORRELATION_KENDALL_P_COLUMN),
                F.count(F.when(pair_cond & q_cond, 1)).alias(CORRELATION_KENDALL_Q_COLUMN),
                F.count(F.when(pair_cond & t_cond, 1)).alias(CORRELATION_KENDALL_T_COLUMN),
                F.count(F.when(pair_cond & u_cond, 1)).alias(CORRELATION_KENDALL_U_COLUMN),
                F.max(
                    F.when(
                        ~F.isnull(CORRELATION_VALUE_1_COLUMN), F.col(ROW_NUMBER_X_Y_COLUMN)
                    ).otherwise(F.lit(0))
                ).alias(CORRELATION_COUNT_OUTPUT_COLUMN),
            )
            .withColumn(CORRELATION_CORR_OUTPUT_COLUMN, corr_expr)
        )

        sdf = sdf.select(
            *[F.col(key) for key in groupKeys],
            *[CORRELATION_CORR_OUTPUT_COLUMN, CORRELATION_COUNT_OUTPUT_COLUMN],
        )
        return sdf
