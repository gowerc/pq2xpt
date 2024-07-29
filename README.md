

# Parquet -> XPT

## Installation

- Clone repo including submodules
```
git clone --recurse-submodules https://github.com/gowerc/pq2xpt.git
```

- Install arrow / parquet
```
# Debian / Ubuntu
sudo apt install -y -V libparquet-dev

# Fedora / RHEL
sudo dnf install -y parquet-devel
```

- Configure and Build
```
cmake -S . -B build
cmake --build build
```

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

- [ ] Lots and lots of testing

- [ ] Proper performance benchmarks