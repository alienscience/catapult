#[allow(unused_imports)]

use serde_json;
use serde_json::value;
use serde_json::Value;

use chrono::prelude::Utc;

#[allow(dead_code)]
fn int_to_level(level: u64) -> String {
    match level {
        10 => "trace".to_string(),
        20 => "debug".to_string(),
        30 => "info".to_string(),
        40 => "warn".to_string(),
        50 => "error".to_string(),
        60 => "fatal".to_string(),
        _ => format!("Unknown level {}", level),
    }
}

#[allow(dead_code)]
pub fn transform(input_value: &mut Value) -> Value {
    // {"name":"stakhanov","hostname":"Quark.local","pid":65470,"level":30
    // "msg":"pushing http://fr.wikipedia.org/wiki/Giant_Sand",
    // "time":"2015-05-21T10:11:02.132Z","v":0}
    //
    // entry['@timestamp'] = entry.time;
    // entry.level = levels[entry.level];
    // entry.message = entry.msg;
    // delete entry.time;
    // delete entry.msg;
    let input = input_value.as_object_mut().unwrap();

    if input.contains_key("time") {
        let time = input.get("time").unwrap().clone();
        input.insert("@timestamp".to_string(), time);
        input.remove("time");
    } else {
        // Inject now timestamp.
        let tm = Utc::now();
        let format = "%Y-%m-%dT%H:%M:%S%.3fZ";
        let timestamp = tm.format(format).to_string();

        input.insert(
            "@timestamp".to_string(),
            value::to_value(&timestamp).unwrap_or(Value::Null),
        );
    }

    if input.contains_key("level") {
        let level = input.get("level").unwrap().as_u64().unwrap();
        input.insert(
            "level".to_string(),
            value::to_value(&int_to_level(level)).unwrap_or(Value::Null),
        );
    }

    if input.contains_key("msg") {
        let message = input.get("msg").unwrap().clone();
        input.insert("message".to_string(), message);
        input.remove("msg");
    }
    return value::to_value(input).unwrap_or(Value::Null);
}

#[allow(dead_code)]
pub fn time_to_index_name(full_timestamp: &str) -> String {
    // compatible with "2015-05-21T10:11:02.132Z"
    let mut input = full_timestamp.to_string();
    input.truncate(10);
    input = input.replace("-", ".");
    format!("logstash-{}", input)
}

#[test]
fn it_transform_ok() {
    let src = r#"{"level":30, "msg":"this is a test.", "time": "12"}"#;
    let mut decode = serde_json::from_str::<Value>(src).unwrap();
    let transformed = transform(&mut decode);
    let out = serde_json::to_string(&transformed).unwrap();
    assert_eq!(
        out,
        r#"{"@timestamp":"12","level":"info","message":"this is a test."}"#
    );
}

#[test]
fn it_prepares_index_name() {
    let src = r#"{"time": "2015-05-21T10:11:02.132Z"}"#;
    let decode = serde_json::from_str::<Value>(src).unwrap();
    match decode.get("time") {
        Some(time) => assert_eq!(
            "logstash-2015.05.21",
            time_to_index_name(time.as_str().unwrap())
        ),
        None => assert!(false),
    }
}
