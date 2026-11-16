use crate::ot::Component;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketType {
    Disconnect = 0,
    Connect = 1,
    Heartbeat = 2,
    Message = 3,
    Json = 4,
    Event = 5,
    Ack = 6,
    Error = 7,
    Noop = 8,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Disconnect,
    Connect,
    Heartbeat,
    Message(String),
    Json(serde_json::Value),
    Event {
        id: Option<String>,
        ack: bool,
        name: String,
        args: Vec<serde_json::Value>,
    },
    Ack {
        id: String,
        args: Vec<serde_json::Value>,
    },
    Error {
        reason: String,
        advice: String,
    },
    Noop,
}

impl Packet {
    pub fn parse(raw: &str) -> Result<Packet, String> {
        let mut parts = raw.splitn(4, ':');
        let type_str = parts.next().ok_or("Missing type")?;
        let id_str = parts.next().unwrap_or("");
        let _endpoint = parts.next().unwrap_or("");
        let data_str = parts.next().unwrap_or("");

        let packet_type = type_str.parse::<u8>().map_err(|e| e.to_string())?;

        match packet_type {
            0 => Ok(Packet::Disconnect),
            1 => Ok(Packet::Connect),
            2 => Ok(Packet::Heartbeat),
            3 => Ok(Packet::Message(data_str.to_string())),
            4 => {
                let val: serde_json::Value =
                    serde_json::from_str(data_str).map_err(|e| e.to_string())?;
                Ok(Packet::Json(val))
            }
            5 => {
                let id = if id_str.is_empty() {
                    None
                } else {
                    Some(id_str.trim_end_matches('+').to_string())
                };
                let ack = id_str.ends_with('+');
                let parsed: EventPayload =
                    serde_json::from_str(data_str).map_err(|e| e.to_string())?;
                Ok(Packet::Event {
                    id,
                    ack,
                    name: parsed.name,
                    args: parsed.args,
                })
            }
            6 => {
                // In Socket.IO 0.9: 6:::<id>+<data> or 6:::<id>
                let (id, args) = if let Some(plus_idx) = data_str.find('+') {
                    let id = data_str[..plus_idx].to_string();
                    let rest = &data_str[plus_idx + 1..];
                    let args = if rest.starts_with('[') {
                        serde_json::from_str(rest).unwrap_or_default()
                    } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(rest) {
                        vec![val]
                    } else {
                        Vec::new()
                    };
                    (id, args)
                } else {
                    (data_str.to_string(), Vec::new())
                };
                Ok(Packet::Ack { id, args })
            }
            7 => Ok(Packet::Error {
                reason: id_str.to_string(),
                advice: data_str.to_string(),
            }),
            8 => Ok(Packet::Noop),
            other => Err(format!("Unknown packet type: {other}")),
        }
    }

    pub fn encode(&self) -> String {
        match self {
            Packet::Disconnect => "0:::".to_string(),
            Packet::Connect => "1:::".to_string(),
            Packet::Heartbeat => "2::".to_string(),
            Packet::Message(msg) => format!("3:::{msg}"),
            Packet::Json(val) => format!("4:::{}", val),
            Packet::Event { id, ack, name, args } => {
                let id_field = match (id, ack) {
                    (Some(id_val), true) => format!("{id_val}+"),
                    (Some(id_val), false) => id_val.clone(),
                    (None, _) => String::new(),
                };
                let payload = EventPayload {
                    name: name.clone(),
                    args: args.clone(),
                };
                let json = serde_json::to_string(&payload).unwrap_or_default();
                format!("5:{id_field}::{json}")
            }
            Packet::Ack { id, args } => {
                let json = serde_json::to_string(args).unwrap_or_default();
                format!("6:::{id}+{json}")
            }
            Packet::Error { reason, advice } => format!("7::{reason}:{advice}"),
            Packet::Noop => "8::".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    pub name: String,
    pub args: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocView {
    pub _id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRefView {
    pub _id: String,
    pub name: String,
    #[serde(default)]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderView {
    pub _id: String,
    pub name: String,
    #[serde(default)]
    pub folders: Vec<FolderView>,
    #[serde(default)]
    pub docs: Vec<DocView>,
    #[serde(default, rename = "fileRefs", alias = "file_refs")]
    pub file_refs: Vec<FileRefView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectView {
    pub _id: String,
    pub name: String,
    #[serde(default, rename = "rootDoc_id", alias = "root_doc_id")]
    pub root_doc_id: Option<String>,
    #[serde(default, rename = "rootFolder", alias = "root_folder")]
    pub root_folder: Vec<FolderView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinProjectResponse {
    pub project: ProjectView,
    #[serde(default, rename = "publicId", alias = "public_id")]
    pub public_id: Option<String>,
    #[serde(default, rename = "permissionsLevel", alias = "permissions_level")]
    pub permissions_level: Option<String>,
    #[serde(default, rename = "protocolVersion", alias = "protocol_version")]
    pub protocol_version: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtUpdatePayload {
    pub doc: String,
    pub v: i64,
    pub op: Vec<Component>,
    #[serde(default)]
    pub meta: Option<OtMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtMeta {
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
}

pub fn encode_join_doc(msg_id: u64, doc_id: &str) -> String {
    Packet::Event {
        id: Some(msg_id.to_string()),
        ack: true,
        name: "joinDoc".to_string(),
        args: vec![
            serde_json::Value::String(doc_id.to_string()),
            serde_json::json!(-1),
            serde_json::json!({}),
        ],
    }
    .encode()
}

pub fn encode_apply_ot(msg_id: u64, doc_id: &str, version: i64, ops: &[Component]) -> String {
    let update = serde_json::json!({
        "doc": doc_id,
        "v": version,
        "op": ops,
    });
    Packet::Event {
        id: Some(msg_id.to_string()),
        ack: true,
        name: "applyOtUpdate".to_string(),
        args: vec![
            serde_json::Value::String(doc_id.to_string()),
            update,
        ],
    }
    .encode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_heartbeat() {
        let p = Packet::parse("2::").unwrap();
        assert_eq!(p, Packet::Heartbeat);
        assert_eq!(p.encode(), "2::");
    }

    #[test]
    fn test_parse_event() {
        let raw = r#"5:1+::{"name":"joinDoc","args":["doc123",-1,{}]}"#;
        let p = Packet::parse(raw).unwrap();
        match &p {
            Packet::Event { id, ack, name, args } => {
                assert_eq!(id.as_deref(), Some("1"));
                assert!(*ack);
                assert_eq!(name, "joinDoc");
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Event packet"),
        }
    }

    #[test]
    fn test_parse_ack() {
        let raw = r#"6:::1+[null,["line1","line2"],0,[]]"#;
        let p = Packet::parse(raw).unwrap();
        match p {
            Packet::Ack { id, args } => {
                assert_eq!(id, "1");
                assert_eq!(args.len(), 4);
            }
            _ => panic!("Expected Ack packet"),
        }
    }
}
