use learn_polars::*;
use polars::prelude::*;
use std::path::PathBuf;

fn main() {
    set_env();

    let file =
        PathBuf::from("./100knocks-preprocess/docker/work/data/P_df_product_full_UTF-8_noh.csv");

    let mut df = CsvReader::from_path(file)
        .unwrap()
        .with_separator(b',')
        .has_header(false)
        .finish()
        .unwrap();

    let _ = &df.set_column_names(&[
        "product_cd",
        "category_major_cd",
        "category_medium_cd",
        "category_small_cd",
        "unit_price",
        "unit_cost",
        "category_major_name",
        "category_medium_name",
        "category_small_name",
    ]);

    println!("{}", df.head(Some(3)));
}
