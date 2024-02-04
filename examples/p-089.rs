// Requirements: features=["lazy", "ramdom"]
use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([col("amount").sum()])
        .filter(col("amount").gt(lit(0)));

    let df_all = read_csv(FileName::Receipt)
        .unwrap()
        .inner_join(
            lf.clone().select([col("customer_id")]),
            col("customer_id"),
            col("customer_id"),
        )
        .with_row_index("id", None)
        .collect()
        .unwrap();

    let df_train = df_all
        .clone()
        .lazy()
        .select([all().sample_frac(lit(0.8), false, true, Some(42))]) // shuffle:true
        .collect()
        .unwrap();

    let df_test = df_all
        .clone()
        .lazy()
        .filter(col("id").is_in(lit(df_train["id"].clone())).not())
        .with_column(all().shuffle(Some(42)))
        .collect()
        .unwrap();

    println!(
        "train: {}({})",
        df_train.shape().0,
        df_train.shape().0 as f64 / df_all.shape().0 as f64
    );
    println!(
        "test : {}({})",
        df_test.shape().0,
        df_test.shape().0 as f64 / df_all.shape().0 as f64
    );

    println!("train: {}", df_train.head(Some(5)));
    println!("test: {}", df_test.head(Some(5)));
}
