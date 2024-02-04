use learn_polars::*;
use polars::prelude::*;

fn main() {
    let s: Series = [0.01].iter().collect();
    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .collect()
        .unwrap()
        .sample_frac(&s, false, true, Some(42))
        .unwrap()
        .head(Some(10));

    println!("{}", df_customer);
}
