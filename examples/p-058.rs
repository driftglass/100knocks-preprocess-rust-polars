// "to_dummies" feature is required for this example.

use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([col("customer_id"), col("gender_cd")])
        .collect()
        .unwrap();

    let dummies = df_customer
        .columns_to_dummies(vec!["gender_cd"], Some("_"), false)
        .unwrap()
        .head(Some(10));

    println!("{}", dummies)
}
