use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();
    
    let lf_store = read_csv(FileName::Store)
        .unwrap()
        .select(&[col("store_cd"), col("store_name")]);

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .inner_join(lf_store, col("store_cd"), col("store_cd"))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
