#include <iostream>
#include <arrow/api.h>
#include <memory>


arrow::Status RunMain() {


    // Builders are the main way to create Arrays in Arrow from existing values that are not
    // on-disk. In this case, we'll make a simple array, and feed that in.
    // Data types are important as ever, and there is a Builder for each compatible type;
    // in this case, int8.
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    // AppendValues, as called, puts 5 values from days_raw into our Builder object.
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
    // We only have a Builder though, not an Array -- the following code pushes out the
    // built up data into a proper Array.
    std::shared_ptr<arrow::Array> days;
    ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());




    // Builders clear their state every time they fill an Array, so if the type is the same,
    // we can re-use the builder. We do that here for month values.
    int8_t months_raw[5] = {1, 3, 5, 7, 1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
    std::shared_ptr<arrow::Array> months;
    ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());


    // Now that we change to int16, we use the Builder for that data type instead.
    arrow::Int16Builder int16builder;
    int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
    std::shared_ptr<arrow::Array> years;
    ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());



    // Now, we want a RecordBatch, which has columns and labels for said columns.
    // This gets us to the 2d data structures we want in Arrow.
    // These are defined by schema, which have fields -- here we get both those object types
    // ready.
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    // Every field needs its name and data type.
    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    // The schema can be built from a vector of fields, and we do so here.
    schema = arrow::schema({field_day, field_month, field_year});


    // With the schema and Arrays full of data, we can make our RecordBatch! Here,
    // each column is internally contiguous. This is in opposition to Tables, which we'll
    // see next.
    std::shared_ptr<arrow::RecordBatch> rbatch;
    // The RecordBatch needs the schema, length for columns, which all must match,
    // and the actual data itself.
    rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

    // print the table
    std::cout << rbatch->ToString();


    // Step 2: Cast the generic array to a specific type array
    std::shared_ptr<arrow::Int16Array> int_array = std::static_pointer_cast<arrow::Int16Array>(years);
    std::cout << int_array->Value(1) << std::endl;


    // (years->GetScalar(2)).Value();


    // arrow::Datum sum;
    // ARROW_ASSIGN_OR_RAISE(sum, arrow::compute::Sum({rbatch->GetColumnByName("Day")}));
    // // Get the kind of Datum and what it holds -- this is a Scalar, with int64.
    // std::cout << "Datum kind: " << sum.ToString() << std::endl
    //           << "Content type: " << sum.type()->ToString()
    //           << std::endl;
    // std::cout << sum.scalar_as<arrow::Int64Scalar>().value << std::endl;


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





