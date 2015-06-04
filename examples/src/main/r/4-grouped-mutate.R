#
# Author:   Daniel Emaasit (@emaasit)
# Purpose: This script shows several ways to create Spark DataFrames 
# Date:    06/04/2015
#

source("./examples/src/main/r/1-data.R")

# Motivating examples ----------------------------------------------------------

planes <- flights %>%
  filter(!is.na(arr_delay)) %>%
  group_by(plane) %>%
  filter(n() > 30)

planes %>%
  mutate(z_delay = (arr_delay - mean(arr_delay)) / sd(arr_delay)) %>%
  filter(z_delay > 5)

planes %>% filter(min_rank(arr_delay) < 5)

# Ranking functions ------------------------------------------------------------

min_rank(c(1, 1, 2, 3))
dense_rank(c(1, 1, 2, 3))
row_number(c(1, 1, 2, 3))

flights %>% group_by(plane) %>% filter(row_number(desc(arr_delay)) <= 2)
flights %>% group_by(plane) %>% filter(min_rank(desc(arr_delay)) <= 2)
flights %>% group_by(plane) %>% filter(dense_rank(desc(arr_delay)) <= 2)

# Lead and lag -----------------------------------------------------------------

daily <- flights %>% 
  group_by(date) %>% 
  summarise(delay = mean(dep_delay, na.rm = TRUE))

daily %>% mutate(delay - lag(delay))
daily %>% mutate(delay - lag(delay))
