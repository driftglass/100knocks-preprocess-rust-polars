use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_epoch").cast(DataType::Int64).alias("sales_ymd"), // i32 -> i64
        ])
        .with_column(
            (col("sales_ymd") * 1000i64.into()) // sec -> msec
                .cast(DataType::Datetime(
                    TimeUnit::Milliseconds,
                    Some("Asia/Tokyo".to_string()), // タイムゾーン設定しない場合はNone
                ))
                .cast(DataType::Date)
                .alias("sales_ymd"),
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
