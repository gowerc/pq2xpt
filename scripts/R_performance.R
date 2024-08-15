

dat2 <- arrow::read_parquet("data/performance.parquet")
haven::write_xpt(dat2, "data/performance.xpt")



time_it <- function(expr) {
    start_time <- Sys.time()
    eval(expr)
    end_time <- Sys.time()
    difftime(end_time, start_time, units = "secs")
}


time_it({
    dat2 <- arrow::read_parquet("data/performance.parquet")
    haven::write_xpt(dat2, "data/performance_r.xpt")
})





dat2 <- arrow::read_parquet("data/simple_data.parquet")
haven::write_xpt(dat2, "data/simple_data_r.xpt")

haven::read_xpt("data/simple_data_r.xpt")
haven::read_xpt("data/simple_data_cpp.xpt")




dat_r <- haven::read_xpt("data/performance_r.xpt")
dat_cpp <- haven::read_xpt("data/performance_cpp.xpt")

dat_r
dat_cpp
all.equal(dat_r, dat_cpp)

attributes(dat_r$character_1)
attributes(dat_cpp$character_1)

