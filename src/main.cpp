
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




template <typename DataType,
          typename ArrayType = typename arrow::TypeTraits<DataType>::ArrayType,
          typename CType = typename arrow::TypeTraits<DataType>::CType>
struct ChunkedArrayParser {
    std::shared_ptr<arrow::ChunkedArray> chunked_array;
    std::shared_ptr<arrow::Array> current_array;
    int chunk_index{0};
    int array_index{-1};
    int max_chunk_index;
    int total_elements{0};
    int next_element_index{0};
    std::string type;
    ChunkedArrayParser(std::shared_ptr<arrow::ChunkedArray> chunked_array) {
        this->max_chunk_index = chunked_array->num_chunks();
        this->current_array = chunked_array->chunk(0);
        this->chunked_array = chunked_array;
        for (int i = 0; i < this->max_chunk_index; i++) {
            auto arr = chunked_array->chunk(i);
            this->total_elements += arr->length();
        }
        this->type = chunked_array->type()->ToString();
    }
    CType get_next_element() {
        this->array_index++;
        this->next_element_index++;
        if (this->array_index >= current_array->length()) {
            this->array_index = 0;
            this->chunk_index++;
            if (this->chunk_index >= max_chunk_index) {
                std::cout << "ERROR no more data to give" << std::endl;
            }
            this->current_array = this->chunked_array->chunk(this->chunk_index);
        }
        std::shared_ptr<ArrayType> current_array_typed = std::static_pointer_cast<ArrayType>(this->current_array);
        return current_array_typed->Value(this->array_index);
    }
};



struct VariableMeta {
    std::string name;
    readstat_type_e type;
    std::string format;
    int storage_width;
    std::string label;
};



struct I_Variable {
    VariableMeta meta;
    I_Variable(VariableMeta meta): meta{meta}{};
    virtual void write_xpt_value(readstat_writer_t *writer, readstat_variable_t *reference) =0;
    virtual ~I_Variable() = default;
};



// struct VarBoolean: public I_Variable {
//     ChunkedArrayParser<arrow::BooleanType> parser;
//     VarBoolean(std::string varname, ChunkedArrayParser<arrow::BooleanType> arr):
//         parser{arr},
//         meta{VariableMeta{varname, READSTAT_TYPE_INT8, "", 8, ""}}{};

//     virtual void write_xpt_value(readstat_writer_s *writer, readstat_variable_s *reference) override {
//         readstat_insert_int8_value(writer, reference, this->parser.get_next_element());
//     }
//     virtual ~VarBoolean(){};
// };


struct VarInt32: public I_Variable {
    ChunkedArrayParser<arrow::Int32Type> parser;
    VarInt32(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> arr
    ):
        I_Variable(VariableMeta{varname, READSTAT_TYPE_INT32, "", 8, ""}),
        parser{ChunkedArrayParser<arrow::Int32Type>(arr)}
    {};

    virtual void write_xpt_value(readstat_writer_s *writer, readstat_variable_s *reference) override {
        int x {this->parser.get_next_element()};
        std::cout << "Writing x = " << x << std::endl;
        readstat_insert_int32_value(writer, reference, x);
    }
    virtual ~VarInt32() = default;
};


struct VarDouble: public I_Variable {
    ChunkedArrayParser<arrow::DoubleType> parser;
    VarDouble(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> arr
    ):
        I_Variable(VariableMeta{varname, READSTAT_TYPE_DOUBLE, "", 8, ""}),
        parser{ChunkedArrayParser<arrow::DoubleType>(arr)}
    {};

    virtual void write_xpt_value(readstat_writer_s *writer, readstat_variable_s *reference) override {
        readstat_insert_double_value(writer, reference, this->parser.get_next_element());
    }
    virtual ~VarDouble() = default;
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
    std::vector<std::shared_ptr<I_Variable>> data,
    int row_count
) {

    std::ofstream outputFile {filename,  std::ios::out | std::ios::binary };
    if (!outputFile) {
        std::cerr << "Failed to open file." << std::endl;
        return;
    }

    readstat_writer_t *writer = readstat_writer_init();
    readstat_set_data_writer(writer, &write_bytes);
    readstat_writer_set_file_label(writer, "My data set");

    const int col_count = data.size();


    std::vector<readstat_variable_t*> references;
    references.reserve(col_count);
    std::cout << "ref size = " << references.size() << std::endl;



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
            data.at(j)->write_xpt_value(writer, references.at(j));
        }
        readstat_end_row(writer);
    }
    readstat_end_writing(writer);
    readstat_writer_free(writer);
    outputFile.close();
    return ;
}



arrow::Status RunMain() {
    std::shared_ptr<arrow::Table> parquet_table;
    ARROW_ASSIGN_OR_RAISE(parquet_table, read_parquet("data/performance.parquet"));

    std::vector<std::string> column_names = parquet_table->ColumnNames();

    std::vector<std::shared_ptr<I_Variable>> pqdata;

    for (int i = 1; i < column_names.size(); i++) {
        std::cout << column_names.at(i) << std::endl;
        pqdata.push_back(
            std::make_shared<VarDouble>(
                VarDouble(
                    column_names.at(i),
                    parquet_table->GetColumnByName(column_names.at(i))
                )
            )
        );
    }


    // std::cout << x1->parser.next_element_index << std::endl;
    // std::cout << x1->parser.get_next_element() << std::endl;
    // std::cout << x1->parser.get_next_element() << std::endl;
    // std::cout << x1->parser.get_next_element() << std::endl;


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


