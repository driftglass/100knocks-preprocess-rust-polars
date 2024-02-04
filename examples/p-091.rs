use learn_polars::*;
use polars::prelude::*;

/// `flag_col`列の要素数が少ない方に合わせてアンダーサンプリング
/// - flag_col: bool型のカラム名
fn under_sampling(df: &DataFrame, flag_col: &str) -> PolarsResult<DataFrame> {
    let mask = df.column(flag_col)?.bool()?;
    let s: Series = match &mask.sum() < &(!mask).sum() {
        true => [(mask).sum().unwrap() as i64].iter().collect(),
        false => [(!mask).sum().unwrap() as i64].iter().collect(),
    };
    df.group_by([flag_col])?
        .apply(|x| x.sample_n(&s, false, true, Some(42)))
}

fn main() {
    set_env();

    let lf_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([col("amount").sum()]);

    let df_tmp = read_csv(FileName::Customer)
        .unwrap()
        .left_join(lf_receipt, col("customer_id"), col("customer_id"))
        .with_column(col("amount").is_not_null().alias("is_buy_flag")) // 購入履歴がある人をtrue
        .collect()
        .unwrap();

    let df_down_sampling = under_sampling(&df_tmp, "is_buy_flag").unwrap();

    println!(
        "{:?}",
        &df_down_sampling
            .column("is_buy_flag")
            .unwrap()
            .value_counts(false, false)
    );
    // println!("{}", &df_down_sampling.tail(Some(5)));
}
