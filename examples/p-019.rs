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
                    method: RankMethod::Min,
                    descending: true,
                },
                None,
            )
            .alias("ranking")])
        // customer_id, ranking の順でソートする
        .sort_by_exprs(
            vec![col("ranking"), col("customer_id")], // 右側から処理される
            vec![false, false],
            true,
            true,
        )
        .select(&[col("customer_id"), col("amount"), col("ranking")])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_receipt);
}
