use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .filter(col("customer_id").str().ends_with(lit("1")))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
