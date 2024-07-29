#include <iostream>
#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

// #include <arrow/api.h>
// #include <arrow/io/api.h>
// #include <parquet/arrow/reader.h>
// #include <parquet/arrow/writer.h>

// #include <vector>
#include <functional>

// #include <arrow/compute/api.h>



class I_Writer {};
class DoubleParser : public I_Writer {};
class Int32Parser : public I_Writer {};
class StringParser : public I_Writer {};


int main() {
    std::vector<std::shared_ptr<I_Writer>> pqdata;

    std::unordered_map<std::string, std::function<std::shared_ptr<I_Writer>(const std::string&, const std::shared_ptr<arrow::Column>&)>> parser_factory;
    parser_factory["A"] = []() {
        return std::make_shared<DoubleParser>(DoubleParser(name, column));
    };
    parser_factory["B"] = []() {
        return std::make_shared<Int32Parser>(Int32Parser(name, column));
    };
    parser_factory["C"] = []() {
        return std::make_shared<StringParser>(StringParser(name, column));
    };

    // Loop through columns and create parsers based on their types
    for (int i = 0; i < column_names.size(); i++) {
        auto ColType = parquet_table->column(i)->type()->id();
        auto it = parser_factory.find(ColType);
        if (it != parser_factory.end()) {
            pqdata.push_back(it->second(column_names.at(i), parquet_table->column(i)));
        }
    }
}



// arrow::Status RunMain() {

//     arrow::StringBuilder stringbuilder;
//     std::vector<std::string> sdays_raw = {"A", "              ", "AAA", "D", "E2"};
//     ARROW_RETURN_NOT_OK(stringbuilder.AppendValues(sdays_raw));
//     std::shared_ptr<arrow::Array> sdays;
//     ARROW_ASSIGN_OR_RAISE(sdays, stringbuilder.Finish());

//     std::cout << sdays->ToString() << std::endl;
//     auto carr = std::static_pointer_cast<arrow::StringArray>(sdays);

//     std::cout << max_char_len(carr).ValueOrDie();

//     return arrow::Status::OK();
// }




// int main () { 
//     arrow::Status st = RunMain();
//     if (!st.ok()) {
//         std::cerr << st << std::endl;
//         return 1;
//     }
//     return 0;
// }




