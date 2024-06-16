

# Add submodule to project
git submodule add git@github.com:WizardMac/ReadStat.git external/ReadStat

# Enable submodule (needs to be re-run when cloning project)
git submodule update --init --recursive


# Commands to configure / build code
cmake -S . -B build
cmake --build build


# Run the script
./build/pq2xpt


# cmake --install build   # Centrally install the program / library







cmake -S . -B build;
cmake --build build; ./build/pq2xpt