use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf_category = read_csv(FileName::Category)
        .unwrap()
        .select(&[col("category_small_cd"), col("category_small_name")]);

    let df_product = read_csv(FileName::Product)
        .unwrap()
        .inner_join(
            lf_category,
            col("category_small_cd"),
            col("category_small_cd"),
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_product);
}
