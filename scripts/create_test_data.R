


library(haven)
library(arrow)

dat <- data.frame(
    n1 = rnorm(7),
    c1 = c("A", "B", "C", "D", "E", "F", "G"),
    l1 = c(T, F, T, T, T, F, F)
)

haven::write_xpt(dat, "data/test_dat.xpt")
arrow::write_parquet(dat, "data/test_dat.parquet")


haven::read_xpt("data/test_dat.xpt")
arrow::read_parquet("data/test_dat.parquet")
