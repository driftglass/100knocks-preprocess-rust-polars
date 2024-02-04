// "pivot" feature is required for this example.
use learn_polars::*;
use polars::prelude::pivot::pivot;
use polars::prelude::*;

fn main() {
    set_env();

    // p-043.rsと同じ処理
    let lf_customer = read_csv(FileName::Customer).unwrap().select([
        col("customer_id"),
        col("gender_cd"),
        col("age").alias("era"),
    ]);

    let df = read_csv(FileName::Receipt)
        .unwrap()
        .select([col("customer_id"), col("amount")])
        .inner_join(lf_customer, col("customer_id"), col("customer_id"))
        .with_column(
            when(col("era").is_null())
                .then(0i64)
                .otherwise(
                    (col("era").cast(DataType::Float64) / 10f64.into()).floor() * 10f64.into(),
                )
                .cast(DataType::Int64)
                .alias("era"),
        )
        .collect()
        .unwrap();

    let out = pivot(
        &df,
        ["amount"],
        ["era"],
        ["gender_cd"],
        true,
        Some(col("amount").sum()),
        Some("_"),
    )
    .unwrap()
    .lazy()
    .rename(vec!["0", "1", "9"], vec!["male", "female", "unknown"])
    .sort(
        "era",
        SortOptions {
            descending: false,
            nulls_last: true,
            ..Default::default()
        },
    )
    .collect()
    .unwrap();
    // ここまで

    // pivotの横持ちから縦持ちに変換
    let out = out
        .lazy()
        .rename(vec!["male", "female", "unknown"], vec!["00", "01", "99"])
        .melt(MeltArgs {
            id_vars: vec!["era".into()],
            value_vars: vec!["00".into(), "01".into(), "99".into()],
            variable_name: Some("gender_cd".into()),
            value_name: Some("amount".into()),
            ..Default::default()
        })
        .sort_by_exprs(
            vec![col("era"), col("gender_cd")],
            vec![false, false],
            true,
            true,
        )
        .collect()
        .unwrap();

    println!("{}", out);
}
