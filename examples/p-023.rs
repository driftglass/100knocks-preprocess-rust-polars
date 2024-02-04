use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("store_cd")])
        .agg([
            col("amount").sum().alias("amount"),
            col("quantity").sum().alias("quantity"),
        ])
        .sort(
            "store_cd",
            SortOptions {
                descending: false,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
