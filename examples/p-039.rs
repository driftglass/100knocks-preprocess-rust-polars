use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not());

    let lf_cnt = lf
        .clone()
        .group_by([col("customer_id")])
        .agg([col("sales_ymd").n_unique().alias("sales_ymd")])
        .sort(
            "sales_ymd",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(20);

    let lf_sum = lf
        .clone()
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .sort(
            "amount",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(20);

    let df = lf_cnt
        .outer_join(lf_sum, col("customer_id"), col("customer_id"))
        .sort_by_exprs(
            vec![col("sales_ymd"), col("amount"), col("customer_id")], // 右側から処理される
            vec![true, true, false],
            true,
            true,
        )
        .collect()
        .unwrap();

    println!("{}", df);
}
