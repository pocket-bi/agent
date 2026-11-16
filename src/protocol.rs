use serde::{Deserialize, Serialize};

/// Messages the agent sends to the hub / control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Hello { token: String },
    DialOk { stream_id: String },
    DialErr { stream_id: String, message: String },
    Data { stream_id: String, payload: String },
    Close { stream_id: String },
    Ping,
}

/// Messages the hub / control plane sends to the agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    HelloOk { connector_id: String },
    HelloErr { message: String },
    Dial {
        stream_id: String,
        host: String,
        port: u16,
    },
    Data { stream_id: String, payload: String },
    Close { stream_id: String },
    Pong,
}

impl ClientMsg {
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

impl ServerMsg {
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_hello_roundtrip() {
        let msg = ClientMsg::Hello {
            token: "dev-token".into(),
        };
        let json = msg.to_json().unwrap();
        assert!(json.contains(r#""type":"hello""#));
        assert!(json.contains(r#""token":"dev-token""#));
        let parsed = ClientMsg::from_json(&json).unwrap();
        match parsed {
            ClientMsg::Hello { token } => assert_eq!(token, "dev-token"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn server_dial_roundtrip() {
        let msg = ServerMsg::Dial {
            stream_id: "s1".into(),
            host: "127.0.0.1".into(),
            port: 5432,
        };
        let json = msg.to_json().unwrap();
        let parsed = ServerMsg::from_json(&json).unwrap();
        match parsed {
            ServerMsg::Dial {
                stream_id,
                host,
                port,
            } => {
                assert_eq!(stream_id, "s1");
                assert_eq!(host, "127.0.0.1");
                assert_eq!(port, 5432);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn data_payload_is_opaque_string() {
        let msg = ClientMsg::Data {
            stream_id: "s1".into(),
            payload: "aGVsbG8=".into(), // base64 "hello"
        };
        let parsed = ClientMsg::from_json(&msg.to_json().unwrap()).unwrap();
        match parsed {
            ClientMsg::Data { payload, .. } => assert_eq!(payload, "aGVsbG8="),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_type() {
        assert!(ClientMsg::from_json(r#"{"type":"nope"}"#).is_err());
        assert!(ServerMsg::from_json(r#"{"type":"nope"}"#).is_err());
    }
}
