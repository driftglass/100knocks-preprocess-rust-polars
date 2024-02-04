use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .unique(
            Some(vec!["customer_id".to_string()]),
            UniqueKeepStrategy::First,
        )
        .collect()
        .unwrap();

    println!("ユニーク件数: {}", df_receipt.shape().0);
}
