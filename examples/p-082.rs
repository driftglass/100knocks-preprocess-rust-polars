use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .select([
            all().exclude(["unit_price", "unit_cost"]),
            col("unit_price").fill_null(col("unit_price").median().round(0).cast(DataType::Int64)),
            col("unit_cost").fill_null(col("unit_cost").median().round(0).cast(DataType::Int64)),
        ])
        .collect()
        .unwrap()
        .null_count();

    println!("{}", df_product);
}
