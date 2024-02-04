use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([
            col("customer_id"),
            col("sales_ymd")
                .cast(DataType::String)
                .str()
                // strftime()を使用するためにdate型ではなくdatetime型に変換
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None, // Some("Asia/Tokyo".to_string()),
                    StrptimeOptions {
                        format: Some("%Y%m%d".to_string()),
                        ..Default::default()
                    },
                    lit("raise"), // "raise", "latest", "earliest"
                ),
        ])
        .unique(
            Some(vec!["customer_id".into(), "sales_ymd".into()]), // 重複を削除
            UniqueKeepStrategy::First,
        )
        .left_join(
            read_csv(FileName::Customer).unwrap().select([
                col("customer_id"),
                col("application_date")
                    .cast(DataType::String)
                    .str()
                    // strftime()を使用するためにdate型ではなくdatetime型に変換
                    .to_datetime(
                        Some(TimeUnit::Milliseconds),
                        None, // Some("Asia/Tokyo".to_string()),
                        StrptimeOptions {
                            format: Some("%Y%m%d".to_string()),
                            ..Default::default()
                        },
                        lit("raise"), // "raise", "latest", "earliest"
                    ),
            ]),
            col("customer_id"),
            col("customer_id"),
        )
        .with_columns(vec![
            col("sales_ymd")
                .dt()
                .strftime("%s")
                .cast(DataType::Int32)
                .alias("sales_ymd_epoch"), // Datetime -> epoch(32bit)
            col("application_date")
                .dt()
                .strftime("%s")
                .cast(DataType::Int32)
                .alias("application_date_epoch"), // Datetime -> epoch(32bit)
        ])
        .select([
            col("customer_id"),
            col("sales_ymd").cast(DataType::Date),
            col("application_date").cast(DataType::Date),
            (col("sales_ymd_epoch") - col("application_date_epoch")).alias("elapsed_epoch"),
        ])
        .filter(col("elapsed_epoch").is_not_null()) // Zで始まる顧客IDは非会員のためapplication_dateがnullとなる
        .sort(
            "customer_id",
            SortOptions {
                descending: false,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
