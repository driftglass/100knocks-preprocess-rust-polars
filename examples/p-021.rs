use learn_polars::*;
// use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt).unwrap().collect().unwrap();

    println!("件数: {}", df_receipt.shape().0);
}
