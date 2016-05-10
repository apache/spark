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

# For this example, we shall use the "flights" dataset
# The dataset consists of every flight departing Houston in 2011.
# The data set is made up of 227,496 rows x 14 columns. 

# To run this example use
# ./bin/spark-submit examples/src/main/r/data-manipulation.R <path_to_csv>

# Load SparkR library into your R session
library(SparkR)

args <- commandArgs(trailing = TRUE)

if (length(args) != 1) {
  print("Usage: data-manipulation.R <path-to-flights.csv>")
  print("The data can be downloaded from: http://s3-us-west-2.amazonaws.com/sparkr-data/flights.csv")
  q("no")
}

## Initialize SparkContext
sc <- sparkR.init(appName = "SparkR-data-manipulation-example")

## Initialize SQLContext
sqlContext <- sparkRSQL.init(sc)

flightsCsvPath <- args[[1]]

# Create a local R dataframe
flights_df <- read.csv(flightsCsvPath, header = TRUE)
flights_df$date <- as.Date(flights_df$date)

## Filter flights whose destination is San Francisco and write to a local data frame
SFO_df <- flights_df[flights_df$dest == "SFO", ] 

# Convert the local data frame into a SparkDataFrame
SFO_DF <- createDataFrame(sqlContext, SFO_df)

#  Directly create a SparkDataFrame from the source data
flightsDF <- read.df(sqlContext, flightsCsvPath, source = "csv", header = "true")

# Print the schema of this SparkDataFrame
printSchema(flightsDF)

# Cache the SparkDataFrame
cache(flightsDF)

# Print the first 6 rows of the SparkDataFrame
showDF(flightsDF, numRows = 6) ## Or
head(flightsDF)

# Show the column names in the SparkDataFrame
columns(flightsDF)

# Show the number of rows in the SparkDataFrame
count(flightsDF)

# Select specific columns
destDF <- select(flightsDF, "dest", "cancelled")

# Using SQL to select columns of data
# First, register the flights SparkDataFrame as a table
registerTempTable(flightsDF, "flightsTable")
destDF <- sql(sqlContext, "SELECT dest, cancelled FROM flightsTable")

# Use collect to create a local R data frame
local_df <- collect(destDF)

# Print the newly created local data frame
head(local_df)

# Filter flights whose destination is JFK
jfkDF <- filter(flightsDF, "dest = \"JFK\"") ##OR
jfkDF <- filter(flightsDF, flightsDF$dest == "JFK")

# If the magrittr library is available, we can use it to
# chain data frame operations
if("magrittr" %in% rownames(installed.packages())) {
  library(magrittr)

  # Group the flights by date and then find the average daily delay
  # Write the result into a SparkDataFrame
  groupBy(flightsDF, flightsDF$date) %>%
    summarize(avg(flightsDF$dep_delay), avg(flightsDF$arr_delay)) -> dailyDelayDF

  # Print the computed SparkDataFrame
  head(dailyDelayDF)
}

# Stop the SparkContext now
sparkR.stop()
