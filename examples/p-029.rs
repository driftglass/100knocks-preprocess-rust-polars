// "mode" feature is required for this example.
use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("store_cd")])
        .agg([col("product_cd").mode().get(0).alias("product_cd")])
        .sort(
            "store_cd",
            SortOptions {
                descending: false,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
