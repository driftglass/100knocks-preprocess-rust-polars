use learn_polars::*;
// use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .null_count()
        .collect()
        .unwrap();

    println!("{}", df_product);
}
