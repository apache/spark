#
# Author:   Daniel Emaasit (@emaasit)
# Purpose: This script shows several ways to create Spark DataFrames 
# Date:    06/04/2015
#

source("./examples/src/main/r/1-data.R")

sfo <- filter(flights, dest == "SFO")
qplot(date, dep_delay, data = sfo)
qplot(date, arr_delay, data = sfo)
qplot(arr_delay, dep_delay, data = sfo)

qplot(dep_delay, data = flights, binwidth = 10)
qplot(dep_delay, data = flights, binwidth = 1) + xlim(0, 250)

by_day <- group_by(flights, date)
daily_delay <- summarise(by_day, 
  dep = mean(dep_delay, na.rm = TRUE),
  arr = mean(arr_delay, na.rm = TRUE)
)
qplot(date, dep, data = daily_delay, geom = "line")
qplot(date, arr, data = daily_delay, geom = "line")

# What's the best way to measure delay? ---------------------------------------
daily_delay <- by_day %>% 
  filter(!is.na(dep_delay)) %>%
  summarise(
    mean = mean(dep_delay),
    median = median(dep_delay),
    q75 = quantile(dep_delay, 0.75),
    over_15 = mean(dep_delay > 15),
    over_30 = mean(dep_delay > 30),
    over_60 = mean(dep_delay > 60)
  )

qplot(date, mean, data = daily_delay)
qplot(date, median, data = daily_delay)
qplot(date, q75, data = daily_delay)
qplot(date, over_15, data = daily_delay)
qplot(date, over_30, data = daily_delay)
qplot(date, over_60, data = daily_delay)
