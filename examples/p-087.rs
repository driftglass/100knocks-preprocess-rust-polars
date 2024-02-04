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

    let n0 = df_customer.shape().0;
    let n1 = df_customer_unique.shape().0;

    println!("顧客データ: {}", n0);
    println!("名寄せデータ:  {}", n1);
    println!("重複数: {}", n0 - n1);
}
