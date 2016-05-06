#
# Author:   Daniel Emaasit (@emaasit)
# Purpose: This script shows how to explore and manipulate Spark DataFrames 
# Date:    06/04/2015
#

source("./examples/src/main/r/1-data.R")


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
destDF <- sql(sqlCtx, "SELECT dest, cancelled FROM flightsTable")

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
