use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("store_cd")])
        .agg([col("amount").median().alias("amount")])
        .sort(
            "amount",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(5)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
