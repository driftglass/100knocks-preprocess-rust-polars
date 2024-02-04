use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    // ヨコ積みの場合
    let df_store = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("sales_ymd")])
        .agg([col("amount").sum().alias("amount")])
        .sort_by_exprs(vec![col("sales_ymd")], vec![false], true, true)
        .with_columns([
            col("amount").shift(Expr::from(1)).alias("amount_prev1"),
            col("amount").shift(Expr::from(2)).alias("amount_prev2"),
            col("amount").shift(Expr::from(3)).alias("amount_prev3"),
        ])
        .drop_nulls(None)
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_store);
}
