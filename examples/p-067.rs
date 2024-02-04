use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .drop_nulls(None)
        .select([
            col("product_cd"),
            col("unit_price").cast(DataType::Float64),
            col("unit_cost").cast(DataType::Float64),
        ])
        .with_column((col("unit_cost") / lit(0.7)).ceil().alias("new_price"))
        .with_column(((col("new_price") - col("unit_cost")) / col("new_price")).alias("new_profit"))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_product);
}
