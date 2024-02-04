use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_store = read_csv(FileName::Store)
        .unwrap()
        .filter(
            col("tel_no")
                .str()
                .contains(lit("^[0-9]{3}-[0-9]{3}-[0-9]{4}$"), false),
        )
        .collect()
        .unwrap();

    println!("{}", df_store);
}
