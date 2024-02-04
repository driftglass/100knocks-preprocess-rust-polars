use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_store = read_csv(FileName::Store)
        .unwrap()
        .filter(
            col("prefecture_cd")
                .neq(lit("13"))
                .and(col("floor_area").lt_eq(900)),
        )
        .collect()
        .unwrap();

    println!("{}", df_store);
}
