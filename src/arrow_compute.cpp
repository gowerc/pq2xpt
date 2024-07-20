#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <memory>


arrow::Status RunMain() {

    //////////////////////////
    //
    // Generate a table for testing with
    //
    //////////////////////////

    // Create a couple 32-bit integer arrays.
    arrow::Int32Builder int32builder;
    int32_t some_nums_raw[5] = {1, 2, 3, 4, 5};
    ARROW_RETURN_NOT_OK(int32builder.AppendValues(some_nums_raw, 5));
    std::shared_ptr<arrow::Array> some_nums;
    ARROW_ASSIGN_OR_RAISE(some_nums, int32builder.Finish());

    int32_t more_nums_raw[5] = {10, 20, 30, 40, 50};
    ARROW_RETURN_NOT_OK(int32builder.AppendValues(more_nums_raw, 5));
    std::shared_ptr<arrow::Array> more_nums;
    ARROW_ASSIGN_OR_RAISE(more_nums, int32builder.Finish());

    // Make a table out of our pair of arrays.
    std::shared_ptr<arrow::Field> field_a, field_b;
    std::shared_ptr<arrow::Schema> schema;

    field_a = arrow::field("A", arrow::int32());
    field_b = arrow::field("B", arrow::int32());

    schema = arrow::schema({field_a, field_b});

    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, {some_nums, more_nums}, 5);





    //////////////////////////
    //
    std::cout << "\n----- Using the sum function -----\n" << std::endl;
    //
    //////////////////////////

    // The Datum class is what all compute functions output to, and they can take Datums
    // as inputs, as well.
    arrow::Datum sum;

    // Here, we can use arrow::compute::Sum. This is a convenience function, and the next
    // computation won't be so simple. However, using these where possible helps
    // readability.
    ARROW_ASSIGN_OR_RAISE(sum, arrow::compute::Sum({table->GetColumnByName("A")}));

    // Get the kind of Datum and what it holds -- this is a Scalar, with int64.
    
    std::cout << "Datum kind: " << sum.ToString()
              << " content type: " << sum.type()->ToString()
              << std::endl;

    // Note that we explicitly request a scalar -- the Datum cannot simply give what it is,
    // you must ask for the correct type.
    std::cout << sum.scalar_as<arrow::Int64Scalar>().value << std::endl;


    //////////////////////////
    //
    std::cout << "\n----- Element-wise functions -----\n" << std::endl;
    //
    //////////////////////////

    arrow::Datum element_wise_sum;
    // Get element-wise sum of both columns A and B in our Table.
    //  e.g.   element_wise_sum_i = A_i + B_i
    // Note that here we use
    // CallFunction(), which takes the name of the function as the first argument.
    // "add" is the name of the function to call. Full list of supported functions can be 
    // found here:
    //     https://arrow.apache.org/docs/cpp/compute.html#arithmetic-functions
    ARROW_ASSIGN_OR_RAISE(
        element_wise_sum,
        arrow::compute::CallFunction(
            "add",
            {
                table->GetColumnByName("A"),
                table->GetColumnByName("B")
            }
        )
    );
    // Get the kind of Datum and what it holds -- this is a ChunkedArray, with int32.
    std::cout << "Datum kind: " << element_wise_sum.ToString()
              << " content type: " << element_wise_sum.type()->ToString() << std::endl;



    //////////////////////////
    //
    std::cout << "\n----- Searching for a particular value -----\n" << std::endl;
    //
    //////////////////////////

    arrow::Datum third_item;

    // An options struct is used in lieu of passing an arbitrary amount of arguments.
    arrow::compute::IndexOptions index_options;

    // We need an Arrow Scalar, not a raw value.
    index_options.value = arrow::MakeScalar(4);

    ARROW_ASSIGN_OR_RAISE(
        third_item, arrow::compute::CallFunction(
            "index",
            {table->GetColumnByName("A")},
            &index_options
        )
    );

    // Get the kind of Datum and what it holds -- this is a Scalar, with int64
    std::cout << "Datum kind: " << third_item.ToString()
                << " content type: " << third_item.type()->ToString() << std::endl;

    // We get a scalar -- the location of 4 in column A, which is 3 in 0-based indexing.
    std::cout << third_item.scalar_as<arrow::Int64Scalar>().value << std::endl;

    return arrow::Status::OK();
}


int main() {
    std::cout << "\n\n ========== Program Start ========== \n" << std::endl;
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    std::cout << "\n\n ========== Program End ========== \n" << std::endl;
    return 0;
}