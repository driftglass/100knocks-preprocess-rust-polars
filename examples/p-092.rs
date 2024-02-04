use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf_customer = read_csv(FileName::Customer).unwrap();

    let df_gender_std = lf_customer
        .clone()
        .select([col("gender_cd"), col("gender")])
        .unique_stable(
            Some(vec!["gender_cd".into(), "gender".into()]), // 重複を削除
            UniqueKeepStrategy::First,
        )
        .collect()
        .unwrap();

    let df_customer_std = lf_customer.clone().drop(vec!["gender"]).collect().unwrap();

    println!("{}", df_gender_std);
    println!("{}", df_customer_std.head(Some(5)));
}
