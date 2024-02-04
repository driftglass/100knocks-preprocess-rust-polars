use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")]);

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .filter(col("gender_cd").eq(lit("1")))
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .left_join(lf_receipt, col("customer_id"), col("customer_id"))
        .with_column(col("amount").fill_null(lit(0)))
        .select(&[col("customer_id"), col("amount")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
