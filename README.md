

# Parquet -> XPT


## TODOs

- [ ] Understand what on earth `std::function` is doing

- [ ] Enable Command line arguements for specifying input file
- [ ] Improve code re-usability (templates?) to reduce duplication in the parsers

- [ ] Improve error handling (need ability to correctly abort if user has an unparsable type)

- [ ] Support Logical data types
- [ ] Support factors
- [ ] Support Date types (+ ns varient)
- [ ] Support Datetime types (+ ns varient)
- [ ] Support time stamps
- [ ] Support all int variants
- [ ] Support all float variants

- [ ] Review what other types need to be supported
    - [ ] Create python script that generates all parquet types and see how R handles the xpt mapping

- [ ] `haven::write_xpt()` converts Inf / -Inf to NA.
    - [ ] Need to check if this is expected behaviour as it appears XPT does support Inf/-Inf (though need to check how SAS interprets these values)

