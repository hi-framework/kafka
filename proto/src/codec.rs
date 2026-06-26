use crate::frame::{FrameType, HEADER_LEN, MAX_PAYLOAD_LEN};
use bytes::{BufMut, BytesMut};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("payload too large: {0} > {max}", max = MAX_PAYLOAD_LEN)]
    PayloadTooLarge(usize),

    #[error("unknown frame type: 0x{0:02x}")]
    UnknownFrameType(u8),

    #[error("header buffer too short: {0} < {HEADER_LEN}")]
    HeaderTooShort(usize),
}

/// 帧头解析结果
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub payload_len: u32,
    pub kind: FrameType,
    pub cid: u64,
}

/// 把帧写入 `buf`。返回写入的总字节数（含 header）。
pub fn encode_frame(
    kind: FrameType,
    cid: u64,
    payload: &[u8],
    buf: &mut BytesMut,
) -> Result<usize, CodecError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(CodecError::PayloadTooLarge(payload.len()));
    }
    buf.reserve(HEADER_LEN + payload.len());
    buf.put_u32(payload.len() as u32);
    buf.put_u8(kind as u8);
    buf.put_u64(cid);
    buf.put_slice(payload);
    Ok(HEADER_LEN + payload.len())
}

/// 从 13 字节 header 解析。不消费 payload。
pub fn decode_header(buf: &[u8]) -> Result<Header, CodecError> {
    if buf.len() < HEADER_LEN {
        return Err(CodecError::HeaderTooShort(buf.len()));
    }
    let payload_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let kind = FrameType::from_u8(buf[4]).ok_or(CodecError::UnknownFrameType(buf[4]))?;
    let cid = u64::from_be_bytes([
        buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
    ]);
    if payload_len as usize > MAX_PAYLOAD_LEN {
        return Err(CodecError::PayloadTooLarge(payload_len as usize));
    }
    Ok(Header {
        payload_len,
        kind,
        cid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Frame;
    use bytes::Bytes;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut buf = BytesMut::new();
        let payload = b"hello kafka";
        let written = encode_frame(FrameType::ProduceFnf, 42, payload, &mut buf).unwrap();
        assert_eq!(written, HEADER_LEN + payload.len());

        let header = decode_header(&buf[..HEADER_LEN]).unwrap();
        assert_eq!(header.payload_len as usize, payload.len());
        assert_eq!(header.kind, FrameType::ProduceFnf);
        assert_eq!(header.cid, 42);
        assert_eq!(&buf[HEADER_LEN..], payload);
    }

    #[test]
    fn test_empty_payload() {
        let mut buf = BytesMut::new();
        encode_frame(FrameType::Ping, 1, b"", &mut buf).unwrap();
        let header = decode_header(&buf).unwrap();
        assert_eq!(header.payload_len, 0);
        assert_eq!(header.kind, FrameType::Ping);
        assert_eq!(header.cid, 1);
    }

    #[test]
    fn test_payload_too_large() {
        let mut buf = BytesMut::new();
        let huge = vec![0u8; MAX_PAYLOAD_LEN + 1];
        let err = encode_frame(FrameType::ProduceFnf, 0, &huge, &mut buf).unwrap_err();
        assert!(matches!(err, CodecError::PayloadTooLarge(_)));
    }

    #[test]
    fn test_unknown_frame_type() {
        let mut buf = [0u8; HEADER_LEN];
        buf[4] = 0xFF;
        let err = decode_header(&buf).unwrap_err();
        assert!(matches!(err, CodecError::UnknownFrameType(0xFF)));
    }

    #[test]
    fn test_header_too_short() {
        let buf = [0u8; HEADER_LEN - 1];
        let err = decode_header(&buf).unwrap_err();
        assert!(matches!(err, CodecError::HeaderTooShort(_)));
    }

    #[test]
    fn test_frame_struct() {
        let f = Frame::new(FrameType::Hello, 0, Bytes::from_static(b"hi"));
        assert_eq!(f.kind, FrameType::Hello);
        assert_eq!(f.cid, 0);
        assert_eq!(&f.payload[..], b"hi");
    }
}
