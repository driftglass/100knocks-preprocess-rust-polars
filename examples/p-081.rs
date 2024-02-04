use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .collect()
        .unwrap()
        .fill_null(FillNullStrategy::Mean)
        .unwrap()
        .null_count();

    println!("{}", df_product);
}
