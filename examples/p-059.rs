use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .filter(col("customer_id").str().starts_with(lit("Z")).not())
        .group_by([col("customer_id")])
        .agg([col("amount").sum().alias("amount")])
        .with_column(
            ((col("amount") - col("amount").mean()) / col("amount").std(0)).alias("std_amount"),
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
