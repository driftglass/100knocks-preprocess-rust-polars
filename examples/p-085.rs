use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .left_join(
            read_csv(FileName::Geocode)
                .unwrap()
                .group_by([col("postal_cd")])
                .agg([
                    col("longitude").mean().alias("longitude"),
                    col("latitude").mean().alias("latitude"),
                ]),
            col("postal_cd"),
            col("postal_cd"),
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
