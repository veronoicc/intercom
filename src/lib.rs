#[cfg(not(any(feature = "ring", feature = "aws-lc-rs")))]
compile_error!("Either the ring or aws-lc-rs feature must be enabled");

#[cfg(not(any(feature = "json", feature = "msgpack")))]
compile_error!("Either the json or msgpack feature must be enabled");

use std::marker::PhantomData;

pub use async_nats as nats;
use nats::{subject::ToSubject, ToServerAddrs};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub mod subscriber;
pub use subscriber::Subscriber;

pub trait Encoder {}

#[cfg_attr(feature = "json", derive(Debug, Clone, Copy))]
#[cfg(feature = "json")]
pub struct JsonEncoder;
#[cfg(feature = "json")]
impl Encoder for JsonEncoder {}
#[cfg(feature = "json")]
pub type JsonIntercom = Intercom<JsonEncoder>;

#[cfg_attr(feature = "msgpack", derive(Debug, Clone, Copy))]
#[cfg(feature = "msgpack")]
pub struct MessagePackEncoder;
#[cfg(feature = "msgpack")]
impl Encoder for MessagePackEncoder {}
#[cfg(feature = "msgpack")]
pub type MessagePackIntercom = Intercom<MessagePackEncoder>;

pub struct JetStream;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Encoding {
    NoEncoding,
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "msgpack")]
    MessagePack,
}

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

#[derive(Clone, Debug)]
pub struct Intercom<E> {
    inner: nats::Client,
    encoding: Encoding,
    _phantom: PhantomData<E>,
}

impl From<nats::Client> for Intercom<()> {
    fn from(inner: nats::Client) -> Self {
        Self {
            inner,
            encoding: Encoding::NoEncoding,
            _phantom: PhantomData,
        }
    }
}

impl Intercom<()> {
    pub async fn connect<A: ToServerAddrs>(uri: A) -> Result<Self, async_nats::Error> {
        let inner = nats::connect(uri).await?;
        Ok(Self::from(inner))
    }

    pub async fn connect_with_options<A: ToServerAddrs>(
        uri: A,
        options: nats::ConnectOptions,
    ) -> Result<Self, async_nats::Error> {
        let inner = nats::connect_with_options(uri, options).await?;
        Ok(Self::from(inner))
    }

    #[cfg(feature = "json")]
    pub fn json(self) -> Intercom<JsonEncoder> {
        Intercom {
            inner: self.inner,
            encoding: Encoding::Json,
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "msgpack")]
    pub fn message_pack(self) -> Intercom<MessagePackEncoder> {
        Intercom {
            inner: self.inner,
            encoding: Encoding::MessagePack,
            _phantom: PhantomData,
        }
    }
}

impl<E: Encoder> Intercom<E> {
    pub async fn publish<S: ToSubject, T: Serialize>(
        &self,
        subject: S,
        payload: T,
    ) -> Result<(), async_nats::Error> {
        let payload = match self.encoding {
            Encoding::NoEncoding => unreachable!(),
            #[cfg(feature = "json")]
            Encoding::Json => serde_json::to_vec(&payload)?,
            #[cfg(feature = "msgpack")]
            Encoding::MessagePack => rmp_serde::to_vec(&payload)?,
        };

        self.inner.publish(subject, payload.into()).await?;

        Ok(())
    }

    pub async fn request<S: ToSubject, TI: Serialize, TO: DeserializeOwned>(
        &self,
        subject: S,
        payload: TI,
    ) -> Result<Message<TO>, async_nats::Error> {
        let payload = match self.encoding {
            Encoding::NoEncoding => unreachable!(),
            #[cfg(feature = "json")]
            Encoding::Json => serde_json::to_vec(&payload)?,
            #[cfg(feature = "msgpack")]
            Encoding::MessagePack => rmp_serde::to_vec(&payload)?,
        };

        let msg = self.inner.request(subject, payload.into()).await?;

        let payload = match self.encoding {
            Encoding::NoEncoding => unreachable!(),
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
