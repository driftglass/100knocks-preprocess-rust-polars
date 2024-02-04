use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .select([
            all().exclude(["unit_price", "unit_cost"]),
            coalesce(&[
                col("unit_price"),
                col("unit_price")
                    .median()
                    .round(0)
                    .over([col("category_small_cd")])
                    .cast(DataType::Int64),
            ]),
            coalesce(&[
                col("unit_cost"),
                col("unit_cost")
                    .median()
                    .round(0)
                    .over([col("category_small_cd")])
                    .cast(DataType::Int64),
            ]),
        ])
        .collect()
        .unwrap()
        .null_count();

    println!("{}", df_product);
}
