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
        .select([
            col("customer_id"),
            col("birth_day"),
            col("age")
                .map(|s| Ok(Some(calc_era(&s))), GetOutput::default())
                .alias("era"),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
