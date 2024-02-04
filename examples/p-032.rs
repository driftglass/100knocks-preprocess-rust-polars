use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .select([
            col("amount")
                .quantile(Expr::from(0.25), QuantileInterpolOptions::Nearest)
                .alias("25%"),
            col("amount")
                .quantile(Expr::from(0.5), QuantileInterpolOptions::Nearest)
                .alias("50%"),
            col("amount")
                .quantile(Expr::from(0.75), QuantileInterpolOptions::Nearest)
                .alias("75%"),
            col("amount")
                .quantile(Expr::from(1.0), QuantileInterpolOptions::Nearest)
                .alias("100%"),
        ])
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
