use serde::{Deserialize, Deserializer, Serializer};

pub fn is_zero(x: &u64) -> bool {
    *x == 0
}

// DataHub serializes entity ids as JSON strings (ids are 64-bit and would lose
// precision in JavaScript clients when sent as numbers). These helpers keep the
// Rust-side type numeric while putting a string on the wire in both directions.
// Deserialization also accepts a JSON number, so older payloads still parse.

macro_rules! opt_string_id {
    ($modname:ident, $t:ty) => {
        pub mod $modname {
            use super::*;

            pub fn serialize<S: Serializer>(value: &Option<$t>, s: S) -> Result<S::Ok, S::Error> {
                match value {
                    Some(v) => s.serialize_str(&v.to_string()),
                    None => s.serialize_none(),
                }
            }

            pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<$t>, D::Error> {
                #[derive(Deserialize)]
                #[serde(untagged)]
                enum StringOrNumber {
                    Str(String),
                    Num($t),
                }
                Ok(match Option::<StringOrNumber>::deserialize(d)? {
                    Some(StringOrNumber::Str(s)) => {
                        Some(s.parse().map_err(serde::de::Error::custom)?)
                    }
                    Some(StringOrNumber::Num(n)) => Some(n),
                    None => None,
                })
            }
        }
    };
}

opt_string_id!(opt_string_id, u64);
opt_string_id!(opt_string_id_i64, i64);

/// Non-optional `u64` id as a JSON string (accepts string or number on input).
pub mod string_id {
    use super::*;

    pub fn serialize<S: Serializer>(value: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrNumber {
            Str(String),
            Num(u64),
        }
        match StringOrNumber::deserialize(d)? {
            StringOrNumber::Str(s) => s.parse().map_err(serde::de::Error::custom),
            StringOrNumber::Num(n) => Ok(n),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Sample {
        #[serde(default, with = "super::opt_string_id")]
        id: Option<u64>,
        #[serde(default, with = "super::string_id")]
        unit_id: u64,
        #[serde(default, with = "super::opt_string_id_i64")]
        parent_id: Option<i64>,
    }

    #[test]
    fn serializes_ids_as_strings() {
        // 2^53 + 1 — would lose precision as a JSON number in JavaScript.
        let s = Sample { id: Some(9_007_199_254_740_993), unit_id: 42, parent_id: Some(-5) };
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, r#"{"id":"9007199254740993","unit_id":"42","parent_id":"-5"}"#);
    }

    #[test]
    fn deserializes_from_string() {
        let s: Sample =
            serde_json::from_str(r#"{"id":"123","unit_id":"42","parent_id":"-5"}"#).unwrap();
        assert_eq!(s, Sample { id: Some(123), unit_id: 42, parent_id: Some(-5) });
    }

    #[test]
    fn deserializes_from_number_for_backcompat() {
        let s: Sample = serde_json::from_str(r#"{"id":123,"unit_id":42,"parent_id":-5}"#).unwrap();
        assert_eq!(s, Sample { id: Some(123), unit_id: 42, parent_id: Some(-5) });
    }

    #[test]
    fn handles_missing_and_null() {
        let s: Sample = serde_json::from_str(r#"{"unit_id":"7","id":null}"#).unwrap();
        assert_eq!(s, Sample { id: None, unit_id: 7, parent_id: None });
    }
}
