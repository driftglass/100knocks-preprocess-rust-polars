use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf_category = read_csv(FileName::Category)
        .unwrap()
        .select([col("category_small_cd"), col("^category_.*_name$")]); // カラム名を正規表現で指定する場合はlit()でラップする必要はない

    let df_product_full = read_csv(FileName::Product)
        .unwrap()
        .left_join(
            lf_category,
            col("category_small_cd"),
            col("category_small_cd"),
        )
        .collect()
        .unwrap();

    println!("{}", df_product_full.head(Some(10)));
}
