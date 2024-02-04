use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let lf_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .group_by([col("customer_id")])
        .agg([
            col("amount")
                .filter(
                    col("sales_ymd")
                        .gt_eq(lit(20190101))
                        .and(col("sales_ymd").lt_eq(lit(20191231))),
                )
                .sum()
                .alias("amount_2019"),
            col("amount").sum().alias("amount_all"),
        ])
        .with_column(
            (col("amount_2019").cast(DataType::Float64)
                / col("amount_all").cast(DataType::Float64))
            .alias("amount_rate"),
        );

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .select([col("customer_id")])
        .left_join(lf_receipt, col("customer_id"), col("customer_id"))
        .fill_null(0)
        .filter(col("amount_rate").gt(lit(0)))
        .collect()
        .unwrap();

    let check = df_customer.null_count();

    println!("{}", df_customer.head(Some(10)));
    println!("{}", check);
}
