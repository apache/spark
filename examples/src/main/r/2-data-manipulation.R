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


# Load SparkR library into your R session
library(SparkR)

## Initialize SparkContext on your local PC
sc <- sparkR.init(master = "local", appName = "MyApp")

## Initialize SQLContext
sqlContext <- SparkRSQL.init(sc)

# For this example, we shall use the "flights" dataset
# The data can be downloaded from: https://s3-us-west-2.amazonaws.com/sparkr-data/flights.csv 
# The dataset consists of every flight departing Houston in 2011.
# The data set is made up of 227,496 rows x 14 columns. 


# Option 1: Create an R data frame and then convert it to a SparkR DataFrame -------

## Create R dataframe
install.packages("data.table") #We want to use the fread() function to read the dataset
library(data.table)

flights_df <- fread("flights.csv")
flights_df$date <- as.Date(flights_df$date)

## Convert the local data frame into a SparkR DataFrame
flightsDF <- createDataFrame(sqlContext, flights_df)

# Option 2: Alternatively, directly create a SparkR DataFrame from the source data
flightsDF <- read.df(sqlContext, "flights.csv", source = "csv", header = "true")

# Print the schema of this Spark DataFrame
printSchema(flightsDF)

# Cache the DataFrame
cache(flightsDF)


# Install the magrittr pipeline operator
install.packages("magrittr")
library(magrittr)

# Print the first 6 rows of the DataFrame
showDF(flightsDF, numRows = 6) ## Or
head(flightsDF)

# Show the column names in the DataFrame
columns(flightsDF)

# Show the number of rows in the DataFrame
count(flightsDF)

# Show summary statistics for numeric colums
Describe(flightsDF)

# Select specific columns
destDF <- select(flightsDF, "dest", "cancelled")

# Using SQL to select columns of data
# First, register the flights DataFrame as a table
registerTempTable(flightsDF, "flightsTable")
destDF <- sql(sqlContext, "SELECT dest, cancelled FROM flightsTable")

# Use collect to create a local R data frame
dest_df <- collect(destDF)

# Print the newly created local data frame
print(dest_df)

# Filter flights whose destination is JFK
jfkDF <- filter(flightsDF, "dest == JFK") ##OR
jfkDF <- filter(flightsDF, flightsDF$dest == JFK)

# Group the flights by date and then find the average daily delay
# Write the result into a DataFrame
groupBy(flightsDF, "date") %>%
  avg(dep_delay = "avg", arr_delay = "avg") -> dailyDelayDF

# Stop the SparkContext now
sparkR.stop()
