use learn_polars::*;
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
    // ここまで

    // pivot前のdfからgroup_byで直接縦持ちに変換できる
    let out = df
        .lazy()
        .group_by(["era", "gender_cd"])
        .agg(vec![col("amount").sum().alias("amount")])
        .with_column(
            when(col("gender_cd").eq(lit("0")))
                .then(lit("00"))
                .when(col("gender_cd").eq(lit("1")))
                .then(lit("01"))
                .otherwise(lit("99"))
                .alias("gender_cd"),
        )
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
