use learn_polars::*;
// use polars::prelude::*;

fn main() {
    let lf_store = read_csv(FileName::Store).unwrap();
    let lf_product = read_csv(FileName::Product).unwrap();

    let df = lf_store.cross_join(lf_product).collect().unwrap();

    println!("{}", df.shape().0);
}
