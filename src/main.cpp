
extern "C" {
#include "readstat.h"
}

#include <fstream>
#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <functional>
#include "conversion.cpp"


#include <stdint.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>



using I_WriterPtr = std::shared_ptr<I_Writer>;
using ChunkedArrayPtr = std::shared_ptr<arrow::ChunkedArray>;
using WriterConstructor = std::function<std::shared_ptr<I_Writer>(std::string, ChunkedArrayPtr)>;





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





// XPT writing function that satisfies the signature:
// ```
// using  readstat_data_writer = ssize_t (*)(const void *data, size_t *len, void *ctx);
// ```
// Note that "*ctx" is a pointer to the "ctx" argument provided to `readstat_begin_writing_xport`
// In this case it will be a pointer to an `std::ofstream` object
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

    //std::string datafile = "data/simple_data";
    std::string datafile = "data/performance";

    std::shared_ptr<arrow::Table> parquet_table;
    ARROW_ASSIGN_OR_RAISE(parquet_table, read_parquet(datafile + ".parquet"));

    std::vector<std::string> column_names = parquet_table->ColumnNames();

    std::vector<I_WriterPtr> pqdata;
    pqdata.reserve(column_names.size());


    for (int i = 0; i < column_names.size(); i++) {

        arrow::Type::type ColType = parquet_table->column(i)->type()->id();
        std::string colname = column_names.at(i);
        auto coldata = parquet_table->column(i);

        switch(ColType) {
            case arrow::Type::DOUBLE:
                pqdata.push_back(std::make_shared<DoubleParser>(colname, coldata));
                break;
            case arrow::Type::INT32:
                pqdata.push_back(std::make_shared<Int32Parser>(colname, coldata));
                break;
            case arrow::Type::STRING:
                pqdata.push_back(std::make_shared<StringParser>(colname, coldata));
                break;
            default:
                std::cout << "Unsupported Column Type" << std::endl;
        }
    }
    write_xpt(datafile + "_cpp.xpt", pqdata, parquet_table->num_rows());

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
