use polars::prelude::*;
use std::path::PathBuf;

/// 環境変数で標準出力フォーマットの設定
pub fn set_env() {
    std::env::set_var("POLARS_FMT_MAX_COLS", "50"); // default: 8
    std::env::set_var("POLARS_FMT_MAX_ROWS", "60"); // default: 8
    std::env::set_var("RUST_BACKTRACE", "full");
}

/// 読込CSVファイルの種類
pub enum FileName {
    Customer,
    Category,
    Product,
    Receipt,
    Store,
    Geocode,
}

impl FileName {
    fn to_filepath(&self) -> PathBuf {
        let name = match self {
            FileName::Customer => "customer",
            FileName::Category => "category",
            FileName::Product => "product",
            FileName::Receipt => "receipt",
            FileName::Store => "store",
            FileName::Geocode => "geocode",
        };
        let mut path = PathBuf::from("./100knocks-preprocess/docker/work/data/");
        path.push(format!("{}.csv", name));
        path.to_str().unwrap().to_string();
        path
    }
}

/// 100knocks-preprocessのCSVファイルを読み込んでLazyFrameを返す
pub fn read_csv(name: FileName) -> PolarsResult<LazyFrame> {
    // Schemaでデータ型の指定
    let mut sc = Schema::new();
    sc.with_column("customer_id".to_string().into(), DataType::String);
    sc.with_column("gender_cd".to_string().into(), DataType::String);
    sc.with_column("postal_cd".to_string().into(), DataType::String);
    sc.with_column("application_store_cd".to_string().into(), DataType::String);
    sc.with_column("status_cd".to_string().into(), DataType::String);
    sc.with_column("category_major_cd".to_string().into(), DataType::String);
    sc.with_column("category_medium_cd".to_string().into(), DataType::String);
    sc.with_column("category_small_cd".to_string().into(), DataType::String);
    sc.with_column("product_cd".to_string().into(), DataType::String);
    sc.with_column("store_cd".to_string().into(), DataType::String);
    sc.with_column("prefecture_cd".to_string().into(), DataType::String);
    sc.with_column("tel_no".to_string().into(), DataType::String);
    sc.with_column("postal_cd".to_string().into(), DataType::String);
    sc.with_column("street".to_string().into(), DataType::String);
    sc.with_column("birth_day".into(), DataType::Date);

    let lf = LazyCsvReader::new(name.to_filepath())
        .with_separator(b',')
        .has_header(true)
        .with_dtype_overwrite(Some(&sc))
        .finish()?;

    Ok(lf)
}
