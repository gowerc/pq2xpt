


library(haven)
library(arrow)
library(tibble)
library(lubridate)



generate_test_data <- function(
    n_row = 100,
    n_col = 3,
    n_integer = n_col,
    n_float = n_col,
    n_character = n_col,
    n_factor = n_col,
    n_logical = n_col,
    n_date = n_col,
    n_datetime = n_col
) {
    dat <- dplyr::tibble(id = 1:n_row)

    for (i in seq_len(n_integer)) {
        dat[sprintf("integer_%i", i)] <- as.integer(sample(-99999999:99999999, n_row, TRUE))
    }

    for (i in seq_len(n_float)) {
        dat[sprintf("float_%i", i)] <- (runif(n_row) - 0.5) * 100
    }

    for (i in seq_len(n_character)) {
        chars <- stringi::stri_rand_strings(10000, 1:40)
        dat[sprintf("character_%i", i)] <- sample(chars, n_row, TRUE)
    }

    for (i in seq_len(n_factor)) {
        chars <- c("A", "BB", "CCC", "DD", "EEEEE", "F1", "G2", "H34", "I567")
        dat[sprintf("factor_%i", i)] <- factor(sample(chars, n_row, TRUE))
    }


    for (i in seq_len(n_logical)) {
        dat[sprintf("logical_%i", i)] <- sample(c(TRUE, FALSE), n_row, TRUE)
    }


    for (i in seq_len(n_date)) {
        dat[sprintf("date_%i", i)] <- today() - runif(n_row, 1, 3000)
    }


    for (i in seq_len(n_datetime)) {
        dat[sprintf("datetime_%i", i)] <- Sys.time() - runif(n_row, 1, 1000000000)
    }
    return(dat)
}



dat <- generate_test_data(
    1000000,
    n_col = 0,
    n_character = 10,
    n_float = 10,
    n_integer = 10,
    n_logical = 10,
)
arrow::write_parquet(dat, "data/performance.parquet")













set.seed(10231)
dat <- tibble(
    int = c(7L:1L),
    number = rnorm(7),
    number_special = c(1, 2, NA_real_, -Inf, Inf, NaN, 999),
    string = c("string 1", "the old his", "!?@#!£$%^\U0001F601\U0001F607", "D", NA_character_, "F", "G"),
    logical = c(T, F, T, NA, T, F, F),
    date = today() - runif(7, 1, 3000),
    datetime = Sys.time()  - runif(7, 1, 900000000),
    factor = factor(c("X", "X", "Y", NA, "Y", "Y", "X"), levels = c("Y", "X", "Z"))
)
arrow::write_parquet(dat, "data/r_data_types.parquet")





dat <- dplyr::tibble(
    number_1 = rnorm(7),
    number_2 = rnorm(7),
    number_3 = c(NA, NA, 4, NaN, 2, Inf, -Inf),
    string_1 = c("A", "", "CCC", "D", NA_character_, "FFFFF  ", "G")
)
arrow::write_parquet(dat, "data/simple_data.parquet")








