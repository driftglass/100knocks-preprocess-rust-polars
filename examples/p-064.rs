use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .with_columns(vec![
            col("unit_price").cast(DataType::Float64),
            col("unit_cost").cast(DataType::Float64),
        ])
        .select(
            [((col("unit_price") - col("unit_cost")) / col("unit_price"))
                .drop_nulls()
                .mean()
                .alias("profit_mean")],
        )
        .collect()
        .unwrap();

    let out = df_product
        .column("profit_mean")
        .unwrap()
        .get(0)
        .unwrap()
        .try_extract::<f64>()
        .unwrap();

    println!("mean: {}", out);
    println!("{}", df_product);
}
