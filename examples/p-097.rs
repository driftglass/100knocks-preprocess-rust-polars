use polars::prelude::*;
use std::path::PathBuf;

fn main() {
    std::env::set_var("POLARS_FMT_MAX_COLS", "50"); // default: 8
    std::env::set_var("POLARS_FMT_MAX_ROWS", "50"); // default: 8
    std::env::set_var("RUST_BACKTRACE", "full");

    let file =
        PathBuf::from("./100knocks-preprocess/docker/work/data/P_df_product_full_UTF-8_header.csv");

    let df = CsvReader::from_path(file)
        .unwrap()
        .with_separator(b',')
        .has_header(true)
        .finish()
        .unwrap();

    println!("{}", df.head(Some(3)));
}
