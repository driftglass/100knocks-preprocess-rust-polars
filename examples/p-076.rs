use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let s: Series = [0.1].iter().collect();
    let sampling = move |df: DataFrame| -> Result<DataFrame, PolarsError> {
        df.sample_frac(&s, false, true, Some(42))
    };

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .group_by([col("gender_cd")])
        .apply(sampling, Schema::new().into())
        .collect()
        .unwrap();

    let df_customer = df_customer
        .lazy()
        .group_by_stable([col("gender_cd")])
        .agg([col("customer_id").count()])
        .sort(
            "gender_cd",
            SortOptions {
                descending: false,
                ..Default::default()
            },
        )
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
