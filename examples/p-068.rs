use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .drop_nulls(None)
        .select([col("product_cd"), col("unit_price").cast(DataType::Float64)])
        .with_column((col("unit_price") * lit(1.1)).floor().alias("tax_price"))
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_product);
}
