


library(haven)
library(arrow)
library(tibble)
library(lubridate)

set.seed(10231)

dat <- tibble(
    int = c(7L:1L),
    number = rnorm(7),
    number_special = c(1, 2, NA_real_, -Inf, Inf, NaN, 999),
    string = c("string 1", "the old his", "!?@#!£$%^\U0001F601\U0001F607", "D", "E", "F", "G"),
    logical = c(T, F, T, T, T, F, F),
    date = today() - runif(7, 1, 3000),
    datetime = Sys.time()  - runif(7, 1, 900000000),
    factor = factor(c("X", "X", "Y", "Z", "Y", "Y", "X"), levels = c("Y", "X", "Z"))
)

haven::write_xpt(dat, "data/r_data_types.xpt")
arrow::write_parquet(dat, "data/r_data_types.parquet")




dat <- tibble(
    number_1 = rnorm(7),
    number_2 = rnorm(7)

)
arrow::write_parquet(dat, "data/simple_data.parquet")


a
nobs <- 1000000
nvar <- 40
dat <- tibble(id = seq_len(nobs))
for (i in 1:nvar) dat[[sprintf("number_%04i", i)]] <- rnorm(nobs)

system.time({
    arrow::write_parquet(dat, "data/performance.parquet")
})



system.time({
    dat2 <- arrow::read_parquet("data/performance.parquet")
    haven::write_xpt(dat2, "data/performance.xpt")
})
#    user  system elapsed 
#   6.697   0.623   6.738 






arrow::read_parquet("data/r_data_types.parquet", as_data_frame = FALSE)



