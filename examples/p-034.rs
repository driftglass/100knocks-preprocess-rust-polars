use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .mean()
        .unwrap()
        .collect()
        .unwrap();

    // AnyValue<'_>をf64にキャスト
    let amount_mean = df_receipt
        .column("amount")
        .unwrap()
        .get(0)
        .unwrap()
        .try_extract::<f64>()
        .unwrap();

    println!("mean: {}", amount_mean);
    println!("{}", df_receipt);
}
