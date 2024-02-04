use learn_polars::*;
use polars::prelude::*;
use std::fs::File;

fn main() {
    let lf_category = read_csv(FileName::Category)
        .unwrap()
        .select([col("category_small_cd"), col("^category_.*_name$")]); // カラム名を正規表現で指定する場合はlit()でラップする必要はない

    let mut df_product_full = read_csv(FileName::Product)
        .unwrap()
        .left_join(
            lf_category,
            col("category_small_cd"),
            col("category_small_cd"),
        )
        .collect()
        .unwrap();

    let mut file =
        File::create("./100knocks-preprocess/docker/work/data/P_df_product_full_UTF-8_header.csv")
            .expect("could not create file");

    let _ = CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df_product_full);
}
