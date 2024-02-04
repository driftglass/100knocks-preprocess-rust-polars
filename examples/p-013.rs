use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .filter(col("status_cd").str().contains(lit("^[A-F]"), false))
        .collect()
        .unwrap();

    println!("{}", df_customer.head(Some(10)));
}
