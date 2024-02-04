use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_store = read_csv(FileName::Store)
        .unwrap()
        .filter(col("store_cd").str().starts_with(lit("S14")))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_store);
}
