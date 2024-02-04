use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([
            col("customer_id"),
            col("application_date")
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

    println!("{}", df_customer);
}
