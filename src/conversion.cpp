

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


#include <arrow/compute/api.h>


#include <cmath>



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
        double value = this->data->Value(i);
        if (this->data->IsNull(i) || isnan(value)) {
            readstat_insert_missing_value(writer, reference);
        } else {
            readstat_insert_double_value(writer, reference, value);
        }
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
        int32_t value = this->data->Value(i);
        if (this->data->IsNull(i)) {
            readstat_insert_missing_value(writer, reference);
        } else {
            readstat_insert_int32_value(writer, reference, value);
        }
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


arrow::Result<int32_t> max_char_len(std::shared_ptr<arrow::StringArray> char_arr) {
    arrow::Datum element_len;
    ARROW_ASSIGN_OR_RAISE(
        element_len,
        arrow::compute::CallFunction( "binary_length", {char_arr})
    );

    arrow::Datum max_len;
    ARROW_ASSIGN_OR_RAISE(
        max_len,
        arrow::compute::CallFunction( "max", {element_len.array()})
    );
    return max_len.scalar_as<arrow::Int32Scalar>().value;
}


struct StringParser: public I_Writer {
    std::shared_ptr<arrow::StringArray> data;
    virtual void write_xpt(
        readstat_writer_s *writer,
        readstat_variable_s *reference,
        int i
    ) override {
        std::string_view value = this->data->Value(i);
        std::string x(value.data(), value.length());
        if (this->data->IsNull(i)) {
            std::cout << "IS NULL" << std::endl;
            readstat_insert_missing_value(writer, reference);
        } else {
            readstat_insert_string_value(writer, reference, x.data());
        }
    }
    StringParser(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> chunked_array
    ):
        I_Writer(VariableMeta{varname, READSTAT_TYPE_STRING, "", -1, ""})
    {
        this->data = std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(0));
        I_Writer::meta.storage_width = max_char_len(this->data).ValueOrDie();
    }
};



struct BoolParser: public I_Writer {
    std::shared_ptr<arrow::BooleanArray> data;
    virtual void write_xpt(
        readstat_writer_s *writer,
        readstat_variable_s *reference,
        int i
    ) override {
        int8_t value = this->data->Value(i);
        if (this->data->IsNull(i)) {
            readstat_insert_missing_value(writer, reference);
        } else {
            readstat_insert_int8_value(writer, reference, value);
        }
    }
    BoolParser(
        std::string varname,
        std::shared_ptr<arrow::ChunkedArray> chunked_array
    ):
        I_Writer(VariableMeta{varname, READSTAT_TYPE_INT8, "", 8, ""})
    {
        this->data = std::static_pointer_cast<arrow::BooleanArray>(chunked_array->chunk(0));
    }
};




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





// struct I_Shape {
//     virtual void draw(int a, int b) = 0;
// };

// struct Square: public I_Shape {
//     void draw(int a, int b) override {<object specific code>};
//     Square(int x, int y) { <object specific code> };
// };

// struct Triangle: public I_Shape {
//     void draw(int a, int b) override {<object specific code>};
//     Triangle(int x, int y) { <object specific code> };
// };



// std::vector<std::shared_ptr<I_Shape>> shapes_ptr;

// for (auto item: data) {
//     if (trait(item) == "A") {
//         shapes_ptr.push_back(
//             std::make_shared<Triangle>(Triangle(item.a, item.b));
//         )
//     }
//     if (trait(item) == "B") {
//         shapes_ptr.push_back(
//             std::make_shared<Square>(Square(item.a, item.b));
//         )
//     }
// }


