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
# The data can be downloaded from: https://s3-us-west-2.amazonaws.com/sparkr-data/flights.csv 
# The dataset consists of every flight departing Houston in 2011.
# The data set is made up of 227,496 rows x 14 columns. 

source("0-getting-started.R")

# Create an R data frame and then convert it to a SparkR DataFrame -------

## Create R dataframe
install.packages("data.table") #We want to use the fread() function to read the dataset
library(data.table)

flights_df <- fread("flights.csv")
flights_df$date <- as.Date(flights_df$date)

## Convert the local data frame into a SparkR DataFrame
flightsDF <- createDataFrame(sqlCtx, flights_df)

## Print the schema of this Spark DataFrame
printSchema(flightsDF)

## Cache the DataFrame
cache(flightsDF)
