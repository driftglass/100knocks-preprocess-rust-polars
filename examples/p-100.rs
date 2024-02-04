use learn_polars::*;
use polars::prelude::*;
use std::path::PathBuf;

fn main() {
    set_env();

    let file =
        PathBuf::from("./100knocks-preprocess/docker/work/data/P_df_product_full_UTF-8_header.tsv");

    let df = CsvReader::from_path(file)
        .unwrap()
        .with_separator(b'\t')
        .has_header(true)
        .finish()
        .unwrap();

    println!("{}", df.head(Some(3)));
}
