

set(READSTAT_DIR "${CMAKE_SOURCE_DIR}/external/ReadStat")

add_library(
    ReadStat
    "${READSTAT_DIR}/src/CKHashTable.c"
    "${READSTAT_DIR}/src/readstat_bits.c"
    "${READSTAT_DIR}/src/readstat_convert.c"
    "${READSTAT_DIR}/src/readstat_error.c"
    "${READSTAT_DIR}/src/readstat_io_unistd.c"
    "${READSTAT_DIR}/src/readstat_malloc.c"
    "${READSTAT_DIR}/src/readstat_metadata.c"
    "${READSTAT_DIR}/src/readstat_parser.c"
    "${READSTAT_DIR}/src/readstat_value.c"
    "${READSTAT_DIR}/src/readstat_variable.c"
    "${READSTAT_DIR}/src/readstat_writer.c"
    "${READSTAT_DIR}/src/sas/ieee.c"
    "${READSTAT_DIR}/src/sas/readstat_sas.c"
    "${READSTAT_DIR}/src/sas/readstat_sas7bcat_read.c"
    "${READSTAT_DIR}/src/sas/readstat_sas7bcat_write.c"
    "${READSTAT_DIR}/src/sas/readstat_sas7bdat_read.c"
    "${READSTAT_DIR}/src/sas/readstat_sas7bdat_write.c"
    "${READSTAT_DIR}/src/sas/readstat_sas_rle.c"
    "${READSTAT_DIR}/src/sas/readstat_xport.c"
    "${READSTAT_DIR}/src/sas/readstat_xport_read.c"
    "${READSTAT_DIR}/src/sas/readstat_xport_write.c"
    "${READSTAT_DIR}/src/sas/readstat_xport_parse_format.c"
)


target_include_directories(
    ReadStat
    PUBLIC
    "${READSTAT_DIR}/src"
    "${READSTAT_DIR}/src/sas"
)





