use learn_polars::*;
// use polars::prelude::*;

fn main() {
    set_env();

    let df_product = read_csv(FileName::Product).unwrap().collect().unwrap();

    let df_product_1 = df_product.clone().drop_nulls::<String>(None).unwrap();

    println!("削除前: {}", df_product.shape().0);
    println!("削除後: {}", df_product_1.shape().0);
}
