use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_column(
            when(
                col("amount")
                    .lt(col("amount").quantile(0.25.into(), QuantileInterpolOptions::Nearest)),
            )
            .then(1)
            .when(
                col("amount")
                    .lt(col("amount").quantile(0.5.into(), QuantileInterpolOptions::Nearest)),
            )
            .then(2)
            .when(
                col("amount")
                    .lt(col("amount").quantile(0.75.into(), QuantileInterpolOptions::Nearest)),
            )
            .then(3)
            .otherwise(4)
            .alias("pct_group"),
        )
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
