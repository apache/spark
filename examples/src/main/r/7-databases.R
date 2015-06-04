#
# Author: Daniel Emaasit (@emaasit)
# 


library(dplyr)
source("./examples/src/main/r/1-data.R")

# Create new sqlite3 database
hflights_db <- src_sqlite("hflights.sqlite3", create = TRUE)

# Copy local data to remote database. Use indexes to make
# joins faster
#
# ?copy_to.src_sql
copy_to(hflights_db, as.data.frame(flights), name = "flights", 
  indexes = list(c("date", "hour"), "plane", "dest", "arr"), temporary = FALSE)
copy_to(hflights_db, as.data.frame(weather), name = "weather", 
  indexes = list(c("date", "hour")), temporary = FALSE)
copy_to(hflights_db, as.data.frame(airports), name = "airports", 
  indexes = list("iata"), temporary = FALSE)
copy_to(hflights_db, as.data.frame(planes), name = "planes", 
  indexes = list("plane"), temporary = FALSE)

# Once you have the data in the database, connect to it, instead of 
# loading data from disk
flights_db <- hflights_db %>% tbl("flights")
weather_db <- hflights_db %>% tbl("weather")
planes_db <- hflights_db %>% tbl("planes")
airports_db <- hflights_db %>% tbl("airports")

# You can now work with these objects like they're local data frames
flights_db %>% filter(dest == "SFO")
flights_db %>% left_join(planes_db)

# Note that you often won't know how many rows you'll get - that's an expensive
# operation (requires running the complete query) and dplyr doesn't do that
# unless it has to.

# Operations are lazy: they don't do anything until you need to see the data
hourly <- flights_db %>%
  filter(!is.na(hour)) %>%
  group_by(date, hour) %>%
  summarise(n = n(), arr_delay = mean(arr_delay))

flights_db %>% 
  filter(!is.na(dep_delay)) %>%
  group_by(date, hour) %>%
  summarise(
    delay = mean(dep_delay),
    n = n()
  ) %>% 
  filter(n > 10) %>% explain()


# Use explain to see SQL and get query plan
explain(hourly)
hourly

# Use collect to pull down all data locally
hourly_local <- collect(hourly)
hourly_local

# (Unfortunately date-time conversion for RSQLite is not great)
