use learn_polars::*;
use polars::prelude::*;

fn distance(lon1: Expr, lat1: Expr, lon2: Expr, lat2: Expr) -> Expr {
    let lon1_rad = lon1 * Expr::pi() / lit(180.0);
    let lat1_rad = lat1 * Expr::pi() / lit(180.0);
    let lon2_rad = lon2 * Expr::pi() / lit(180.0);
    let lat2_rad = lat2 * Expr::pi() / lit(180.0);

    let distance = lit(6371)
        * (lat1_rad.clone().sin() * lat2_rad.clone().sin()
            + lat1_rad.clone().cos() * lat2_rad.clone().cos() * (lon1_rad - lon2_rad).cos())
        .arccos();
    distance
}

fn main() {
    set_env();

    let df_customer = read_csv(FileName::Customer)
        .unwrap()
        .left_join(
            read_csv(FileName::Geocode)
                .unwrap()
                .group_by([col("postal_cd")])
                .agg([
                    col("longitude").mean().alias("longitude"),
                    col("latitude").mean().alias("latitude"),
                ]),
            col("postal_cd"),
            col("postal_cd"),
        )
        .join_builder()
        .with(read_csv(FileName::Store).unwrap())
        .how(JoinType::Left)
        .left_on([col("application_store_cd")])
        .right_on([col("store_cd")])
        .suffix("_store")
        .finish()
        .select([
            col("customer_id"),
            col("address"),
            col("address_store"),
            distance(
                col("longitude"),
                col("latitude"),
                col("longitude_store"),
                col("latitude_store"),
            )
            .alias("distance"),
        ])
        .limit(10)
        .collect()
        .unwrap();

    println!("{}", df_customer);
}
