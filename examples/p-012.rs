use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_store = read_csv(FileName::Store)
        .unwrap()
        .filter(col("address").str().contains(lit("横浜市"), false))
        .collect()
        .unwrap();

    println!("{}", df_store);
}
