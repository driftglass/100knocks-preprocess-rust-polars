use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    // タテ持ち
    let lf = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("sales_ymd")])
        .agg([col("amount").sum().alias("amount")])
        .sort_by_exprs(vec![col("sales_ymd")], vec![false], true, true);

    let mut lf_shift = Vec::new();
    for i in 1..4 {
        let tmp = lf.clone().with_columns([
            col("sales_ymd").shift(Expr::from(i)).alias("ymd_prev"),
            col("amount").shift(Expr::from(i)).alias("amount_prev"),
        ]);
        lf_shift.push(tmp);
    }

    let df = concat(&lf_shift, UnionArgs::default())
        .unwrap()
        .drop_nulls(None)
        .sort_by_exprs(
            vec![col("sales_ymd"), col("ymd_prev")],
            vec![false, false],
            true,
            true,
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df);
}
