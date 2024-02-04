use learn_polars::*;
use polars::prelude::*;

/// DataFrameからデータセットを作成
/// - df: DataFrame
/// - train_size: トレーニングデータのサイズ
/// - test_size: テストデータのサイズ
/// - `slide_window * start_point`: データセットの開始位置
fn df_splitter(
    df: &DataFrame,
    train_size: usize,
    test_size: usize,
    slide_window: i64,
    start_point: i64,
) -> (DataFrame, DataFrame) {
    let train_start = start_point * slide_window;
    let test_start = start_point * slide_window + train_size as i64;
    let train = df.slice(train_start, train_size);
    let test = df.slice(test_start, test_size);
    (train, test)
}

// LazyFrameを分割する場合
fn _lf_splitter(
    lf: &LazyFrame,
    train_size: u32,
    test_size: u32,
    slide_window: i64,
    start_point: i64,
) -> (LazyFrame, LazyFrame) {
    let train_start = start_point * slide_window;
    let test_start = start_point * slide_window + train_size as i64;
    let train = lf.clone().slice(train_start, train_size);
    let test = lf.clone().slice(test_start, test_size);
    (train, test)
}

fn main() {
    set_env();

    let df = read_csv(FileName::Receipt)
        .unwrap()
        .with_column(
            col("sales_ymd")
                .cast(DataType::String)
                .str()
                .to_date(StrptimeOptions {
                    format: Some("%Y%m%d".to_string()),
                    ..Default::default()
                }),
        )
        .with_column(
            col("sales_ymd")
                .dt()
                .truncate(lit("1mo"), "0d".into()) // 各日付を月初に丸める
                .alias("sales_ym"),
        )
        .group_by([col("sales_ym")])
        .agg([col("amount").sum()])
        .sort("sales_ym", SortOptions::default())
        .collect()
        .unwrap();

    let mut datasets = Vec::new();
    for i in 0..3 {
        // train: 12ヶ月, test: 6ヶ月, slide_window: 6ヶ月
        let result = df_splitter(&df, 12, 6, 6, i);
        datasets.push(result);
    }

    for dataset in &datasets {
        println!("train: {}", dataset.0);
        println!("test : {} \n", dataset.1);
    }
}
