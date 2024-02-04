use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([
            col("customer_id"),
            when(
                col("postal_cd")
                    .str()
                    .contains(lit("^1[0-9]{2}|20[0-9]"), false),
            )
            .then(1)
            .otherwise(0)
            .alias("postal_flg"),
        ])
        .inner_join(
            read_csv(FileName::Receipt).unwrap(),
            col("customer_id"),
            col("customer_id"),
        )
        .group_by([col("postal_flg")])
        .agg([col("customer_id").n_unique().alias("customer_id")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer)
}
