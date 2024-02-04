use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .inner_join(
            read_csv(FileName::Product).unwrap(),
            col("product_cd"),
            col("product_cd"),
        )
        .group_by([col("customer_id")])
        .agg([
            (col("quantity") * col("unit_price")).sum().alias("sum_all"),
            (col("quantity") * col("unit_price"))
                .filter(col("category_major_cd").eq(lit("07")))
                .sum()
                .alias("sum_07"),
        ])
        .filter(col("sum_07").neq(0))
        .with_column(
            (col("sum_07").cast(DataType::Float64) / col("sum_all").cast(DataType::Float64))
                .alias("sales_rate"),
        )
        .sort(
            "customer_id",
            SortOptions {
                descending: false,
                nulls_last: true,
                ..Default::default()
            },
        )
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
