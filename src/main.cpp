
extern "C" {
#include "readstat.h"
}

#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include <string>

#include <stdint.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>







// arrow::Result<std::shared_ptr<arrow::Table>> read_parquet(std::string file) {

//     arrow::MemoryPool* pool = arrow::default_memory_pool();
//     // Configure general Parquet reader settings
//     auto reader_properties = parquet::ReaderProperties(pool);
//     reader_properties.set_buffer_size(4096 * 4);
//     reader_properties.enable_buffered_stream();

//     // Configure Arrow-specific Parquet reader settings
//     auto arrow_reader_props = parquet::ArrowReaderProperties(true);
//     arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

//     parquet::arrow::FileReaderBuilder reader_builder;
//     ARROW_RETURN_NOT_OK(
//         reader_builder.OpenFile(file, /*memory_map=*/false, reader_properties)
//     );
//     reader_builder.memory_pool(pool);
//     reader_builder.properties(arrow_reader_props);

//     std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
//     ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());


//     std::shared_ptr<arrow::Table> parquet_table;
//     PARQUET_THROW_NOT_OK(
//         arrow_reader->ReadTable(&parquet_table)
//     );
//     return parquet_table;
// }

arrow::Result<std::shared_ptr<arrow::Table>> read_parquet(std::string file) {

    std::shared_ptr<arrow::io::ReadableFile> infile;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::Table> parquet_table;
    
    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(file)
    );
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader)
    );
    PARQUET_THROW_NOT_OK(
        reader->ReadTable(&parquet_table)
    );
    return parquet_table;
}




struct VariableMeta {
    std::string name;
    readstat_type_e type;
    std::string format;
    int storage_width;
    std::string label;
};



struct I_Writer {
    VariableMeta meta;
    I_Writer(VariableMeta meta): meta{meta}{};
    virtual void write_xpt(readstat_writer_s *writer, readstat_variable_s *reference, int i) =0;
    virtual ~I_Writer() = default;
};



struct DoubleParser: public I_Writer {
    std::shared_ptr<arrow::DoubleArray> data;
    virtual void write_xpt(
        readstat_writer_s *writer,
        readstat_variable_s *reference, int i
    ) override {
        readstat_insert_double_value(writer, reference, this->data->Value(i));
    }
    DoubleParser(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> chunked_array
    ):
        I_Writer(VariableMeta{varname, READSTAT_TYPE_DOUBLE, "", 8, ""})
    {
        this->data = std::static_pointer_cast<arrow::DoubleArray>(chunked_array->chunk(0));
    }
};


struct Int32Parser: public I_Writer {
    std::shared_ptr<arrow::Int32Array> data;
    virtual void write_xpt(
        readstat_writer_s *writer,
        readstat_variable_s *reference,
        int i
    ) override {
        readstat_insert_int32_value(writer, reference, this->data->Value(i));
    }
    Int32Parser(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> chunked_array
    ):
        I_Writer(VariableMeta{varname, READSTAT_TYPE_INT32, "", 8, ""})
    {
        this->data = std::static_pointer_cast<arrow::Int32Array>(chunked_array->chunk(0));
    }
};



/* A callback for writing bytes to your file descriptor of choice */
/* The ctx argument comes from the readstat_begin_writing_xxx function */
static ssize_t write_bytes(const void *data, size_t len, void *ctx) {
    std::ofstream* outputFile {reinterpret_cast<std::ofstream *>(ctx)};
    (*outputFile).write(reinterpret_cast<const char*>(data), len);
    return len;
}


void write_xpt(
    std::string filename,
    std::vector<std::shared_ptr<I_Writer>> &data,
    int row_count
) {
    std::cout << "Starting xpt write" << std::endl;
    std::ofstream outputFile {filename,  std::ios::out | std::ios::binary };
    if (!outputFile) {
        std::cerr << "Failed to open file" << std::endl;
        return;
    }

    readstat_writer_t *writer = readstat_writer_init();
    readstat_set_data_writer(writer, &write_bytes);

    const int col_count = data.size();

    std::vector<readstat_variable_t*> references;
    references.reserve(col_count);

    for (int i = 0; i < col_count; i++) {
        references.push_back(readstat_add_variable(
            writer,
            data.at(i)->meta.name.c_str(),
            data.at(i)->meta.type,
            data.at(i)->meta.storage_width
        ));
        readstat_variable_set_label(references.at(i), data.at(i)->meta.label.c_str());
        readstat_variable_set_format(references.at(i), data.at(i)->meta.format.c_str());
    }

    readstat_begin_writing_xport(writer, &outputFile, row_count);
    for (int i=0; i<row_count; i++) {
        readstat_begin_row(writer);
        for (int j=0; j<col_count; j++) {
            data.at(j)->write_xpt(writer, references[j], i);
        }
        readstat_end_row(writer);
    }
    readstat_end_writing(writer);
    readstat_writer_free(writer);
    outputFile.close();
    std::cout << "Finished xpt write" << std::endl;
    return ;
}



arrow::Status RunMain() {
    std::shared_ptr<arrow::Table> parquet_table;
    ARROW_ASSIGN_OR_RAISE(parquet_table, read_parquet("data/performance.parquet"));

    std::vector<std::string> column_names = parquet_table->ColumnNames();

    std::vector<std::shared_ptr<I_Writer>> pqdata;
    pqdata.reserve(column_names.size());


    for (int i = 0; i < column_names.size(); i++) {
        
        auto ColType = parquet_table->column(i)->type()->id();

        if (ColType== arrow::Type::DOUBLE) {
            pqdata.push_back(
                std::make_shared<DoubleParser>(
                    DoubleParser( column_names.at(i), parquet_table->column(i))
                )
            );
        }
        if (ColType == arrow::Type::INT32) {
            pqdata.push_back(
                std::make_shared<Int32Parser>(
                    Int32Parser( column_names.at(i), parquet_table->column(i))
                )
            );
        }
    }
    write_xpt("data/something2.xpt", pqdata, parquet_table->num_rows());

    return arrow::Status::OK();
}


int main () {
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}




// readstat_insert_int8_value(int8_t value);
// readstat_insert_int16_value(int16_t value);
// readstat_insert_int32_value(int32_t value);
// readstat_insert_float_value(float value);
// readstat_insert_double_value(double value);
// readstat_insert_string_value(const char *value);

// readstat_insert_string_ref(readstat_writer_t *writer, const readstat_variable_t *variable, readstat_string_ref_t *ref);
// readstat_insert_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable);
// readstat_insert_tagged_missing_value(readstat_writer_t *writer, const readstat_variable_t *variable, char tag);


// READSTAT_TYPE_STRING
// READSTAT_TYPE_INT8
// READSTAT_TYPE_INT16
// READSTAT_TYPE_INT32
// READSTAT_TYPE_FLOAT
// READSTAT_TYPE_DOUBLE


// BooleanType    -> bool
// UInt8Type      -> uint8_t
// Int8Type       -> int8_t
// UInt16Type     -> uint16_t
// Int16Type      -> int16_t
// UInt32Type     -> uint32_t
// Int32Type      -> int32_t
// UInt64Type     -> uint64_t
// Int64Type      -> int64_t
// HalfFloatType  -> uint16_t
// FloatType      -> float
// DoubleType     -> double
// DecimalType    -> ??
// Decimal128Type -> ??
// Decimal256Type -> ??


// paqruet -> cpp -> cast_xpt -> disk

// vars.push_back(Variable{"Var2", READSTAT_TYPE_INT32, "DATE9.", 8, "My Label 2"});
// vars.push_back(Variable{"Var3", READSTAT_TYPE_INT32, "DATETIME20.", 8, "My Label 4"});
// vars.push_back(Variable{"Var4", READSTAT_TYPE_STRING, "$", 8, "My Label 5"});


