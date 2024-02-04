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
                .to_date(StrptimeOptions {
                    format: Some("%Y%m%d".to_string()),
                    ..Default::default()
                }),
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
                    .to_date(StrptimeOptions {
                        format: Some("%Y%m%d".to_string()),
                        ..Default::default()
                    }),
            ]),
            col("customer_id"),
            col("customer_id"),
        )
        .select([
            col("customer_id"),
            col("sales_ymd"),
            col("application_date"),
            (((col("sales_ymd").dt().year() - col("application_date").dt().year()) * lit(12))
                + (col("sales_ymd").dt().month().cast(DataType::Int64)
                    - col("application_date").dt().month().cast(DataType::Int64))) // u32 -> i64
            .alias("elapsed_months"),
        ])
        .filter(col("elapsed_months").is_not_null()) // Zで始まる顧客IDは非会員のためapplication_dateがnullとなる
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
