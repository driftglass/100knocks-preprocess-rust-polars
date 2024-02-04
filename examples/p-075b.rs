// features=["random"] is required
use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([all().sample_frac(lit(0.01), false, true, Some(42))])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
