use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg(vec![col("amount").sum()])
        .with_column(col("amount").log(std::f64::consts::E).alias("log_amount"))
        .filter(
            (col("log_amount") - col("log_amount").mean())
                .abs()
                .gt(col("log_amount").std(0) * lit(3)),
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
