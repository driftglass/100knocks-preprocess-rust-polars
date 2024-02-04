use learn_polars::*;
use polars::prelude::*;

fn calc_era(age: &Series) -> Series {
    age.i64()
        .unwrap()
        .into_iter()
        .map(|age| match age {
            Some(age) => {
                let era = ((age as f64 / 10.0).floor() * 10.0) as i64;
                if age >= 60 {
                    return 60i64;
                } else {
                    return era;
                }
            }
            None => 0i64,
        })
        .collect()
}

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .with_column(
            col("age")
                .map(|s| Ok(Some(calc_era(&s))), GetOutput::default())
                .alias("era"),
        )
        .select([
            col("customer_id"),
            col("birth_day"),
            col("gender_cd"), // 確認用
            col("era"),
            fold_exprs(
                lit(""),
                |a, b| Ok(Some(&a + &b)),
                [col("gender_cd"), col("era")],
            )
            .alias("gender_era"),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
