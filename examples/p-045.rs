use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([col("customer_id"), col("birth_day").dt().to_string("%Y%m%d")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
