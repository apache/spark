#
# Author:   Daniel Emaasit (@emaasit)
# Purpose: This script shows how to create Spark DataFrames 
# Date:    06/04/2015
#

# For this example, we shall use the "flights" dataset 
# The dataset consists of every ﬂight departing Houston in 2011.
# The data set is made up of 227,496 rows x 14 columns. 

source("./examples/src/main/r/0-getting-started.R")

# Create an R data frame and then convert it to a SparkR DataFrame -------

## Create R dataframe
install.packages("data.table") #We want to use the fread() function to read the dataset
library(data.table)

flights_df <- fread("./examples/src/main/r/flights.csv")
flights_df$date <- as.Date(flights_df$date)

## Convert the local data frame into a SparkR DataFrame
flightsDF <- createDataFrame(sqlCtx, flights_df)

## Print the schema of this Spark DataFrame
printSchema(flightsDF)

## Cache the DataFrame
cache(flightsDF)