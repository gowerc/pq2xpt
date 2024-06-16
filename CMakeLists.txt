
cmake_minimum_required(VERSION 3.15)

project(pq2xpt)

set(CMAKE_CXX_STANDARD 17)

## Define search path for local `Find<lib>.cmake` files
set(
    CMAKE_MODULE_PATH
    ${CMAKE_MODULE_PATH}
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake
)

## Use standard FindParquet.cmake file
find_package(Parquet REQUIRED)

add_executable(pq2xpt src/main.cpp)


find_package(ReadStat REQUIRED)


target_link_libraries(pq2xpt PRIVATE ReadStat)
target_link_libraries(pq2xpt PRIVATE Parquet::parquet_shared)