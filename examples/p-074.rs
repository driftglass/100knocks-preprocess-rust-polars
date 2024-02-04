use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([col("sales_ymd")
            .cast(DataType::String)
            .str()
            .to_date(StrptimeOptions {
                format: Some("%Y%m%d".to_string()),
                ..Default::default()
            })])
        .unique(
            Some(vec!["sales_ymd".into()]), // 重複を削除
            UniqueKeepStrategy::First,
        )
        .with_columns(vec![
            (col("sales_ymd").dt().weekday() - lit(1)).alias("elapsed_day"),
            col("sales_ymd")
                .dt()
                .truncate(lit("1w"), "0d".into()) // 1週間前の月曜
                .alias("Monday"),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
