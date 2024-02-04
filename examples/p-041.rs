use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_store = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("sales_ymd")])
        .agg([col("amount").sum().alias("amount")])
        .sort_by_exprs(vec![col("sales_ymd")], vec![false], true, true)
        .with_columns([col("amount").shift(Expr::from(1)).alias("lag_amount")])
        .with_columns([(col("amount") - col("lag_amount")).alias("diff_amount")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_store);
}
