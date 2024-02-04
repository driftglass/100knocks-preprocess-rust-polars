use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([
            col("sales_ymd").max().alias("sales_ymd_latest"),
            col("sales_ymd").min().alias("sales_ymd_oldest"),
        ])
        .filter(col("sales_ymd_latest").neq(col("sales_ymd_oldest")))
        .sort(
            "customer_id",
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
