use std::collections::HashMap;

use parquet::basic::{Compression, Encoding};
use serde::{de, Deserialize, Serialize, Serializer};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Parameter {
    pub max_row_group_size: usize,
    #[serde(default)]
    pub dictionary_page_size: Option<usize>,
    pub column_configs: HashMap<String, ColumnConfig>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnConfig {
    #[serde(deserialize_with = "deserialize_encoding")]
    #[serde(serialize_with = "serialize_encoding")]
    #[serde(default)]
    pub encoding: Option<Encoding>,

    #[serde(deserialize_with = "deserialize_compression")]
    #[serde(serialize_with = "serialize_compression")]
    #[serde(default)]
    pub compression: Option<Compression>,
}

fn deserialize_encoding<'de, D>(deserializer: D) -> Result<Option<Encoding>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok(None);
    }
    let encoding = if s.eq_ignore_ascii_case(&Encoding::PLAIN.to_string()) {
        Encoding::PLAIN
    } else if s.eq_ignore_ascii_case(&Encoding::PLAIN_DICTIONARY.to_string()) {
        Encoding::PLAIN_DICTIONARY
    } else if s.eq_ignore_ascii_case(&Encoding::RLE.to_string()) {
        Encoding::RLE
    } else if s.eq_ignore_ascii_case(&Encoding::BIT_PACKED.to_string()) {
        Encoding::BIT_PACKED
    } else if s.eq_ignore_ascii_case(&Encoding::DELTA_BINARY_PACKED.to_string()) {
        Encoding::DELTA_BINARY_PACKED
    } else if s.eq_ignore_ascii_case(&Encoding::DELTA_LENGTH_BYTE_ARRAY.to_string()) {
        Encoding::DELTA_LENGTH_BYTE_ARRAY
    } else if s.eq_ignore_ascii_case(&Encoding::DELTA_BYTE_ARRAY.to_string()) {
        Encoding::DELTA_BYTE_ARRAY
    } else if s.eq_ignore_ascii_case(&Encoding::RLE_DICTIONARY.to_string()) {
        Encoding::RLE_DICTIONARY
    } else if s.eq_ignore_ascii_case(&Encoding::BYTE_STREAM_SPLIT.to_string()) {
        Encoding::BYTE_STREAM_SPLIT
    } else {
        panic!("Unrecognized encoding: {}", s)
    };
    Ok(Some(encoding))
}

fn serialize_encoding<S>(x: &Option<Encoding>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        None => s.serialize_none(),
        Some(x) => s.serialize_str(&x.to_string()),
    }
}

fn deserialize_compression<'de, D>(deserializer: D) -> Result<Option<Compression>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    if s.is_empty() {
        return Ok(None);
    }
    let compression = if s.eq_ignore_ascii_case(&Compression::UNCOMPRESSED.to_string()) {
        Compression::UNCOMPRESSED
    } else if s.eq_ignore_ascii_case(&Compression::SNAPPY.to_string()) {
        Compression::SNAPPY
    } else if s.eq_ignore_ascii_case(&Compression::GZIP.to_string()) {
        Compression::GZIP
    } else if s.eq_ignore_ascii_case(&Compression::LZO.to_string()) {
        Compression::LZO
    } else if s.eq_ignore_ascii_case(&Compression::BROTLI.to_string()) {
        Compression::BROTLI
    } else if s.eq_ignore_ascii_case(&Compression::LZ4.to_string()) {
        Compression::LZ4
    } else if s.eq_ignore_ascii_case(&Compression::ZSTD.to_string()) {
        Compression::ZSTD
    } else if s.eq_ignore_ascii_case(&Compression::LZ4_RAW.to_string()) {
        Compression::LZ4_RAW
    } else {
        panic!("Unrecognized compression: {}", s)
    };
    Ok(Some(compression))
}

fn serialize_compression<S>(x: &Option<Compression>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        None => s.serialize_none(),
        Some(x) => s.serialize_str(&x.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_parameter() {
        let parameter = Parameter {
            max_row_group_size: 0,
            dictionary_page_size: None,
            column_configs: [(
                "a".to_string(),
                ColumnConfig {
                    encoding: Some(Encoding::BIT_PACKED),
                    compression: Some(Compression::UNCOMPRESSED),
                },
            )]
            .into_iter()
            .collect(),
        };
        let string = toml::to_string(&parameter).unwrap();
        let x: Parameter = toml::from_str(&string).unwrap();
        assert_eq!(parameter, x);
    }

    #[test]
    fn test_serialize_column_config() {
        let config = ColumnConfig {
            encoding: Some(Encoding::PLAIN),
            compression: Some(Compression::UNCOMPRESSED),
        };
        let string = toml::to_string(&config).unwrap();
        let x: ColumnConfig = toml::from_str(&string).unwrap();
        assert_eq!(config, x);
    }

    #[test]
    fn test_deserialize_column_config() {
        let s = r#"max_row_group_size = 0
dictionary_page_size = 0

[column_configs.a]
Compression = "UNCOMPRESSED"
"#;
        let x: Parameter = toml::from_str(s).unwrap();
        println!("x: {:?}\n", x);
    }
}
