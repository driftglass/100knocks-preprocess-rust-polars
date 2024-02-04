use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([
            col("receipt_no"),
            col("receipt_sub_no"),
            col("sales_ymd")
                .cast(DataType::String)
                .str()
                .to_date(StrptimeOptions {
                    format: Some("%Y%m%d".to_string()),
                    ..Default::default()
                }),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
