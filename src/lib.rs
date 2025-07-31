mod handshake;
pub mod message;
mod time;

pub fn test() {
    let message = message::Message::new("test");
    message.as_bytes();
}
