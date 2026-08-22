use crate::ServerPhase;
use serde::{Deserialize, Serialize};

pub const PIR_SERVICE_ID: &str = "zakura.spendability-pir.v1";
pub const PIR_CAPABILITY: u64 = 1 << 16;
pub const PIR_STREAM_KIND: u16 = 64;
pub const PIR_STREAM_VERSION: u16 = 1;
pub const FRAME_FLAG_MORE: u16 = 1;
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
pub enum P2PError {
    #[error("invalid message type: {0}")]
    InvalidMessageType(u16),
    #[error("invalid frame flags: {0}")]
    InvalidFlags(u16),
    #[error("message type changed before the final frame")]
    MessageTypeChanged,
    #[error("message exceeds {MAX_MESSAGE_BYTES} bytes")]
    MessageTooLarge,
    #[error("service currently unavailable")]
    ServiceUnavailable,
    #[error("serde error: {0}")]
    SerdeError(String),
    #[error("PIR query failed: {0}")]
    QueryError(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CombinedHealthResponse {
    pub nullifier: SubsystemHealth,
    pub witness: SubsystemHealth,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubsystemHealth {
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_height: Option<u64>,
}

impl From<&ServerPhase> for SubsystemHealth {
    fn from(phase: &ServerPhase) -> Self {
        match phase {
            ServerPhase::Serving => Self {
                phase: "serving".into(),
                current_height: None,
                target_height: None,
            },
            ServerPhase::Syncing {
                current_height,
                target_height,
            } => Self {
                phase: "syncing".into(),
                current_height: Some(*current_height),
                target_height: Some(*target_height),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Message {
    HealthReq = 0,
    NullifierMetadataReq = 1,
    NullifierParamsReq = 2,
    NullifierQueryReq = 3,
    WitnessMetadataReq = 4,
    WitnessBroadcastReq = 5,
    WitnessParamsReq = 6,
    WitnessQueryReq = 7,
    HealthRes = 8,
    NullifierMetadataRes = 9,
    NullifierParamsRes = 10,
    NullifierQueryRes = 11,
    WitnessMetadataRes = 12,
    WitnessBroadcastRes = 13,
    WitnessParamsRes = 14,
    WitnessQueryRes = 15,
    ErrRes = 16,
}

impl Message {
    pub const fn response(self) -> Option<Self> {
        match self {
            Self::HealthReq => Some(Self::HealthRes),
            Self::NullifierMetadataReq => Some(Self::NullifierMetadataRes),
            Self::NullifierParamsReq => Some(Self::NullifierParamsRes),
            Self::NullifierQueryReq => Some(Self::NullifierQueryRes),
            Self::WitnessMetadataReq => Some(Self::WitnessMetadataRes),
            Self::WitnessBroadcastReq => Some(Self::WitnessBroadcastRes),
            Self::WitnessParamsReq => Some(Self::WitnessParamsRes),
            Self::WitnessQueryReq => Some(Self::WitnessQueryRes),
            _ => None,
        }
    }
}

impl From<Message> for u16 {
    fn from(message: Message) -> Self {
        message as u16
    }
}

impl TryFrom<u16> for Message {
    type Error = P2PError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        const MESSAGES: [Message; 17] = [
            Message::HealthReq,
            Message::NullifierMetadataReq,
            Message::NullifierParamsReq,
            Message::NullifierQueryReq,
            Message::WitnessMetadataReq,
            Message::WitnessBroadcastReq,
            Message::WitnessParamsReq,
            Message::WitnessQueryReq,
            Message::HealthRes,
            Message::NullifierMetadataRes,
            Message::NullifierParamsRes,
            Message::NullifierQueryRes,
            Message::WitnessMetadataRes,
            Message::WitnessBroadcastRes,
            Message::WitnessParamsRes,
            Message::WitnessQueryRes,
            Message::ErrRes,
        ];
        MESSAGES
            .get(value as usize)
            .copied()
            .ok_or(P2PError::InvalidMessageType(value))
    }
}

#[derive(Default)]
pub struct MessageDecoder(Option<(Message, Vec<u8>)>);

impl MessageDecoder {
    pub fn push(
        &mut self,
        message_type: u16,
        flags: u16,
        payload: Vec<u8>,
    ) -> Result<Option<(Message, Vec<u8>)>, P2PError> {
        if flags > FRAME_FLAG_MORE {
            return Err(P2PError::InvalidFlags(flags));
        }
        let message_type = Message::try_from(message_type)?;
        let more = flags == FRAME_FLAG_MORE;
        if self
            .0
            .as_ref()
            .is_some_and(|(current, _)| *current != message_type)
        {
            return Err(P2PError::MessageTypeChanged);
        }
        let current_len = self.0.as_ref().map_or(0, |(_, payload)| payload.len());
        if payload.len() > MAX_MESSAGE_BYTES.saturating_sub(current_len) {
            return Err(P2PError::MessageTooLarge);
        }
        let (_, combined) = self.0.get_or_insert((message_type, Vec::new()));
        combined.extend(payload);
        Ok((!more).then(|| self.0.take().expect("message is present")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_contract_and_reassembly() {
        let messages = [
            Message::HealthReq,
            Message::NullifierMetadataReq,
            Message::NullifierParamsReq,
            Message::NullifierQueryReq,
            Message::WitnessMetadataReq,
            Message::WitnessBroadcastReq,
            Message::WitnessParamsReq,
            Message::WitnessQueryReq,
            Message::HealthRes,
            Message::NullifierMetadataRes,
            Message::NullifierParamsRes,
            Message::NullifierQueryRes,
            Message::WitnessMetadataRes,
            Message::WitnessBroadcastRes,
            Message::WitnessParamsRes,
            Message::WitnessQueryRes,
            Message::ErrRes,
        ];
        for (number, message) in messages.into_iter().enumerate() {
            assert_eq!(u16::from(message), number as u16);
            assert_eq!(Message::try_from(number as u16).unwrap(), message);
        }

        let mut decoder = MessageDecoder::default();
        assert_eq!(
            decoder.push(0, 0, vec![]).unwrap(),
            Some((Message::HealthReq, vec![]))
        );
        assert_eq!(
            decoder.push(3, 0, vec![1]).unwrap(),
            Some((Message::NullifierQueryReq, vec![1]))
        );
        assert!(decoder
            .push(3, FRAME_FLAG_MORE, vec![1, 2])
            .unwrap()
            .is_none());
        assert_eq!(
            decoder.push(3, 0, vec![3]).unwrap(),
            Some((Message::NullifierQueryReq, vec![1, 2, 3]))
        );
        assert!(matches!(decoder.push(3, FRAME_FLAG_MORE, vec![]), Ok(None)));
        assert!(matches!(
            decoder.push(4, 0, vec![]),
            Err(P2PError::MessageTypeChanged)
        ));

        let mut decoder = MessageDecoder::default();
        assert!(matches!(
            decoder.push(0, 2, vec![]),
            Err(P2PError::InvalidFlags(2))
        ));
        assert!(decoder
            .push(0, FRAME_FLAG_MORE, vec![0; MAX_MESSAGE_BYTES])
            .unwrap()
            .is_none());
        assert!(matches!(
            decoder.push(0, 0, vec![0]),
            Err(P2PError::MessageTooLarge)
        ));
    }
}
