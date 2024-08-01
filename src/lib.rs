#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
compile_error!("Either the ring or aws-lc-rs feature must be enabled");

#[cfg(not(any(feature = "json", feature = "msgpack")))]
compile_error!("Either the json or msgpack feature must be enabled");

use std::marker::PhantomData;

pub use async_nats as nats;
use async_nats::client::FlushError;
use nats::{subject::ToSubject, ToServerAddrs};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod queue;
pub use queue::Queue;
pub mod subscriber;
pub use subscriber::Subscriber;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T> {
    /// Subject to which message is published to.
    pub subject: nats::Subject,
    /// Optional reply subject to which response can be published by [crate::Subscriber].
    /// Used for request-response pattern with [crate::Client::request].
    pub reply: Option<nats::Subject>,
    /// Payload of the message. Can be any arbitrary data format.
    pub payload: T,
    /// Optional headers.
    pub headers: Option<nats::HeaderMap>,
    /// Optional Status of the message. Used mostly for internal handling.
    pub status: Option<nats::StatusCode>,
    /// Optional [status][crate::Message::status] description.
    pub description: Option<String>,

    pub length: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct HasEncoding;

pub type IntercomBuilder = Intercom<()>;

#[derive(Clone, Copy, Debug)]
enum Encoding {
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "msgpack")]
    MessagePack,
}

#[derive(Clone, Debug)]
pub struct Intercom<E = HasEncoding> {
    nats: nats::Client,
    jetstream: nats::jetstream::Context,
    encoding: Option<Encoding>,
    _phantom: PhantomData<E>,
}

impl From<nats::Client> for Intercom<()> {
    fn from(nats: nats::Client) -> Self {
        let jetstream = nats::jetstream::new(nats.clone());

        Self {
            nats,
            jetstream,
            encoding: None,
            _phantom: PhantomData,
        }
    }
}

impl Intercom<()> {
    pub async fn connect<A: ToServerAddrs>(uri: A) -> Result<Self, async_nats::Error> {
        let nats = nats::connect(uri).await?;
        Ok(Self::from(nats))
    }

    pub async fn connect_with_options<A: ToServerAddrs>(
        uri: A,
        options: nats::ConnectOptions,
    ) -> Result<Self, async_nats::Error> {
        let nats = nats::connect_with_options(uri, options).await?;
        Ok(Self::from(nats))
    }

    #[cfg(feature = "json")]
    pub fn json(self) -> Intercom<HasEncoding> {
        Intercom {
            nats: self.nats,
            jetstream: self.jetstream,
            encoding: Some(Encoding::Json),
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "msgpack")]
    pub fn message_pack(self) -> Intercom<HasEncoding> {
        Intercom {
            nats: self.nats,
            jetstream: self.jetstream,
            encoding: Some(Encoding::MessagePack),
            _phantom: PhantomData,
        }
    }
}

impl Intercom<HasEncoding> {
    pub async fn flush(&self) -> Result<(), FlushError> {
        self.nats.flush().await
    }

    pub async fn publish<S: ToSubject, T: Serialize>(
        &self,
        subject: S,
        payload: T,
    ) -> Result<(), async_nats::Error> {
        let payload = match self.encoding.unwrap() {
            #[cfg(feature = "json")]
            Encoding::Json => serde_json::to_vec(&payload)?,
            #[cfg(feature = "msgpack")]
            Encoding::MessagePack => rmp_serde::to_vec(&payload)?,
        };

        self.nats.publish(subject, payload.into()).await?;

        Ok(())
    }

    pub async fn request<S: ToSubject, TI: Serialize, TO: DeserializeOwned>(
        &self,
        subject: S,
        payload: TI,
    ) -> Result<Message<TO>, async_nats::Error> {
        let payload = match self.encoding.unwrap() {
            #[cfg(feature = "json")]
            Encoding::Json => serde_json::to_vec(&payload)?,
            #[cfg(feature = "msgpack")]
            Encoding::MessagePack => rmp_serde::to_vec(&payload)?,
        };

        let msg = self.nats.request(subject, payload.into()).await?;

        let payload = match self.encoding.unwrap() {
            #[cfg(feature = "json")]
            Encoding::Json => serde_json::from_slice(&msg.payload)?,
            #[cfg(feature = "msgpack")]
            Encoding::MessagePack => rmp_serde::from_slice(&msg.payload)?,
        };

        Ok(Message {
            subject: msg.subject,
            reply: msg.reply,
            payload,
            headers: msg.headers,
            status: msg.status,
            description: msg.description,
            length: msg.length,
        })
    }
}
