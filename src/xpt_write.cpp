
extern "C" {
#include "readstat.h"
}

#include <iostream>
#include <fstream>
#include <string>
#include <vector>


struct Variable {
    std::string name;
    readstat_type_e type;
    std::string format;
    int storage_width;
    std::string label;
    readstat_variable_t* reference = nullptr;
};



/* A callback for writing bytes to your file descriptor of choice */
/* The ctx argument comes from the readstat_begin_writing_xxx function */
static ssize_t write_bytes(const void *data, size_t len, void *ctx) {
    std::ofstream* outputFile {reinterpret_cast<std::ofstream *>(ctx)};
    (*outputFile).write(reinterpret_cast<const char*>(data), len);
    return len;
}

int main(int argc, char *argv[]) {
    readstat_writer_t *writer = readstat_writer_init();
    readstat_set_data_writer(writer, &write_bytes);
    readstat_writer_set_file_label(writer, "My data set");

    int row_count = 5;


    std::vector<Variable> vars;
    vars.push_back(Variable{"Var1", READSTAT_TYPE_INT32, "", 8, "My Label"});
    vars.push_back(Variable{"Var2", READSTAT_TYPE_INT32, "DATE9.", 8, "My Label 2"});
    vars.push_back(Variable{"Var3", READSTAT_TYPE_INT32, "DATETIME20.", 8, "My Label 4"});
    vars.push_back(Variable{"Var4", READSTAT_TYPE_STRING, "$", 8, "My Label 5"});


// READSTAT_TYPE_STRING
// READSTAT_TYPE_INT8
// READSTAT_TYPE_INT16
// READSTAT_TYPE_INT32
// READSTAT_TYPE_FLOAT
// READSTAT_TYPE_DOUBLE



    for (Variable &var: vars) {
        var.reference = readstat_add_variable(
            writer,
            var.name.c_str(),
            var.type,
            var.storage_width
        );
        readstat_variable_set_label(var.reference, var.label.c_str());
        readstat_variable_set_format(var.reference, var.format.c_str());
    }


    std::ofstream outputFile {
        "data/something.xpt",  std::ios::out | std::ios::binary
    };
    if (!outputFile) {
        std::cerr << "Failed to open file." << std::endl;
        return 1;
    }

    std::vector<std::string> string_data {
        "aaaaaaaa",
        "bbbbbbbb",
        "cccc",
        "dd",
        "e"
    };

    readstat_begin_writing_xport(writer, &outputFile, row_count);
    int i;
    for (i=0; i<row_count; i++) {
        readstat_begin_row(writer);

        readstat_insert_int32_value(writer, vars[0].reference, 54 * i);
        readstat_insert_int32_value(writer, vars[1].reference, 2313 * i);
        readstat_insert_int32_value(writer, vars[2].reference, 320200000* i);
        readstat_insert_string_value(writer, vars[3].reference, string_data.at(i).c_str());

        readstat_end_row(writer);
    }


    readstat_end_writing(writer);
    readstat_writer_free(writer);
    outputFile.close();
    return 0;
}
