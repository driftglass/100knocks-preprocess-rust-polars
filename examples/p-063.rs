use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .with_column((col("unit_price") - col("unit_cost")).alias("unit_profit"))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_product);
}
