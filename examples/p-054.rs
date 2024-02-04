use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([
            col("customer_id"),
            col("address"),
            when(col("address").str().contains(lit("埼玉県"), false))
                .then(11)
                .when(col("address").str().contains(lit("千葉県"), false))
                .then(12)
                .when(col("address").str().contains(lit("東京都"), false))
                .then(13)
                .when(col("address").str().contains(lit("神奈川県"), false))
                .then(14)
                .otherwise(0) // 適当な値を入れておく
                .alias("prefecture_cd"),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer)
}
