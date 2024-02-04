use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer).unwrap().collect().unwrap();

    let df_customer_unique = df_customer
        .clone()
        .lazy()
        .left_join(
            read_csv(FileName::Receipt)
                .unwrap()
                .group_by([col("customer_id")])
                .agg([col("amount").sum().alias("amount")]),
            col("customer_id"),
            col("customer_id"),
        )
        .sort_by_exprs(
            vec![col("amount"), col("customer_id")],
            vec![true, false], // 降順, 昇順
            true,
            true,
        )
        .unique(
            Some(vec!["customer_name".into(), "postal_cd".into()]),
            UniqueKeepStrategy::First,
        )
        .collect()
        .unwrap();

    let df_customer = df_customer
        .lazy()
        .join_builder()
        .with(df_customer_unique.lazy().select([
            col("customer_name"),
            col("postal_cd"),
            col("customer_id").alias("integration_id"),
        ]))
        .how(JoinType::Left)
        .on([col("customer_name"), col("postal_cd")])
        .finish()
        .collect()
        .unwrap();

    let cnt = df_customer
        .clone()
        .lazy()
        .select([
            col("customer_id").n_unique(),
            col("integration_id").n_unique(),
            (col("customer_id").n_unique() - col("integration_id").n_unique()).alias("diff"),
        ])
        .collect()
        .unwrap();

    let diff = df_customer
        .clone()
        .lazy()
        .filter(col("customer_id").neq(col("integration_id")))
        .collect()
        .unwrap();

    println!("{}", cnt);
    println!("{}", diff);
}
