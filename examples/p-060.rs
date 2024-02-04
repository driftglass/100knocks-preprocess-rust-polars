use learn_polars::*;
use polars::prelude::*;

fn minmax_scale(amount: &Series) -> Option<Series> {
    let min = amount.min::<f64>().unwrap();
    let max = amount.max::<f64>().unwrap();
    match min {
        Some(min) => match max {
            Some(max) => {
                let scale_amount = (amount - min) / (max - min);
                return Some(scale_amount);
            }
            None => None,
        },
        None => None,
    }
}

fn main() {
    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_column(
            col("amount")
                .cast(DataType::Float64)
                .map(|s| Ok(minmax_scale(&s)), GetOutput::default())
                .alias("scale_amount"),
        )
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
