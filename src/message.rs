use std::mem::MaybeUninit;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::time::get_current_timestamp;

pub mod codec;

#[derive(Debug, Clone)]
pub struct Message {
    id: MaybeUninit<u64>,
    timestamp: u64,
    body: Bytes,
}

impl Message {
    pub fn new<B: Into<Bytes>>(body: B) -> Self {
        // Timestamp should be initialized by the event bus
        let id = MaybeUninit::zeroed();
        let timestamp = get_current_timestamp();
        let body = body.into();
        Self {
            id,
            timestamp,
            body,
        }
    }

    pub fn into_bytes(self) -> Bytes {
        self.as_bytes()
    }

    pub fn as_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        // SAFETY: this is save since we initialize it with zeros and therefore unitialized
        // version should be equal to 0.
        unsafe {
            bytes.put_u64(self.id.assume_init_read());
        }
        bytes.put_u64(self.timestamp);
        bytes.put(self.body.clone());

        bytes.freeze()
    }

    pub fn id(&self) -> u64 {
        // SAFETY: this is save since we initialize it with zeroed and therefore unitialized
        // version should be equal to 0
        unsafe { self.id.assume_init_read() }
    }

    pub fn set_id(&mut self, id: u64) {
        self.id.write(id);
    }

    pub fn data(&self) -> &[u8] {
        self.body.as_ref()
    }

    pub fn parse(mut bytes: BytesMut) -> Self {
        let id = MaybeUninit::new(bytes.get_u64());
        let timestamp = bytes.get_u64();
        let body = bytes.freeze();
        Self {
            id,
            timestamp,
            body,
        }
    }
}
