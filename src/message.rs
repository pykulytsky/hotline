use std::mem::MaybeUninit;

use bytes::{BufMut, Bytes, BytesMut};

use crate::time::get_current_timestamp;

pub mod codec;

#[derive(Debug, Clone)]
pub struct Message {
    pub(crate) id: MaybeUninit<u64>,
    pub(crate) timestamp: u64,
    pub(crate) key: String,
    pub(crate) body: Bytes,
}

impl Message {
    pub fn new<B: Into<Bytes>, Key: ToString>(body: B, key: Key) -> Self {
        // Timestamp should be initialized by the message bus
        let id = MaybeUninit::zeroed();
        let timestamp = get_current_timestamp();
        let key = key.to_string();
        let body = body.into();
        Self {
            id,
            timestamp,
            key,
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
        bytes.put_u32(self.key.as_bytes().len() as u32);
        bytes.put(self.key.as_bytes());
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
}
