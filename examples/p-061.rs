use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_columns(vec![
            col("amount")
                .cast(DataType::Float64)
                .log(10.0) // log10(x)
                .alias("log10(x)"),
            col("amount")
                .cast(DataType::Float64)
                .map(|s| Ok(Some((s + 0.5).log(10.0))), GetOutput::default()) // log10(x+0.5)
                .alias("log10(x+0.5)"),
        ])
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
