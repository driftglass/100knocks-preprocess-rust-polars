use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .group_by([col("customer_id")])
        .agg(vec![col("amount").sum()])
        .with_columns(vec![
            col("amount")
                .quantile(0.25.into(), QuantileInterpolOptions::Nearest)
                .alias("q1"),
            col("amount")
                .quantile(0.75.into(), QuantileInterpolOptions::Nearest)
                .alias("q3"),
        ])
        .with_column((col("q3") - col("q1")).alias("iqr"))
        .filter(
            (col("amount").lt(col("q1") - lit(1.5) * col("iqr")))
                .or(col("amount").gt(col("q3") + lit(1.5) * col("iqr"))),
        )
        .select([col("customer_id"), col("amount")])
        .sort(
            "customer_id",
            SortOptions {
                descending: false,
                ..Default::default()
            },
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
