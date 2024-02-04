// "rank" featureが必要(Cargo.tomlに追加)
use learn_polars::*;
use polars::prelude::*;

fn main() {
    set_env();

    let df_receipt = read_csv(FileName::Receipt)
        .unwrap()
        .with_columns([col("amount")
            .rank(
                RankOptions {
                    method: RankMethod::Ordinal,
                    descending: true,
                },
                None,
            )
            .alias("ranking")])
        .sort(
            "ranking",
            SortOptions {
                descending: false,
                nulls_last: true,
                ..Default::default()
            },
        )
        .select(&[col("customer_id"), col("amount"), col("ranking")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
