use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([
            col("sales_ymd"),
            col("customer_id"),
            col("product_cd"),
            col("amount"),
        ])
        .filter(col("customer_id").eq(lit("CS018205000001")))
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
