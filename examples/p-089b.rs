// Requires: features=["partitio_by", "ramdom"]
use learn_polars::*;
use polars::prelude::*;

/// `id`列を`frac`と`1-frac`に分割して上書き
/// - id: ID列
/// - frac: 分割比率
fn id_to_group(id: &Series, frac: f64) -> Series {
    id.cast(&DataType::Float64)
        .unwrap()
        .f64()
        .unwrap()
        .into_iter()
        .map(|i| match i {
            Some(i) => {
                if i < frac * id.len() as f64 {
                    return frac;
                } else {
                    return 1f64 - frac;
                }
            }
            None => 0f64,
        })
        .collect()
}

fn main() {
    set_env();

    let lf = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([col("amount").sum()])
        .filter(col("amount").gt(lit(0)));

    let df_sales_customer = read_csv(FileName::Receipt)
        .unwrap()
        .inner_join(
            lf.clone().select([col("customer_id")]),
            col("customer_id"),
            col("customer_id"),
        )
        .with_column(all().shuffle(Some(42)))
        .with_row_index("group", None)
        .with_column(col("group").map(|s| Ok(Some(id_to_group(&s, 0.8))), GetOutput::default()))
        .collect()
        .unwrap()
        .partition_by_stable(vec!["group"], true)
        .unwrap();

    let df_train = &df_sales_customer[0];
    let df_test = &df_sales_customer[1];

    println!(
        "Train: n={} ({})",
        df_train.shape().0,
        df_train.shape().0 as f64 / (df_train.shape().0 + df_test.shape().0) as f64
    );
    println!(
        "Test: n={} ({})",
        df_test.shape().0,
        df_test.shape().0 as f64 / (df_train.shape().0 + df_test.shape().0) as f64
    );

    println!("train: {}", df_train.head(Some(5)));
    println!("test: {}", df_test.head(Some(5)));
}
