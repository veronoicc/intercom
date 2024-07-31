use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use async_nats::jetstream::{
    consumer::pull::{Config as ConsumerConfig, Stream as Source},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::Stream;
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{nats, Encoding, HasEncoding, Intercom, Message};

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("NATS error: {0}")]
    Nats(nats::Error),
    #[cfg(feature = "json")]
    #[error("JSON error: {0}")]
    Json(serde_json::Error),
    #[cfg(feature = "msgpack")]
    #[error("MessagePack error: {0}")]
    MessagePack(rmp_serde::decode::Error),
}

pin_project! {
    pub struct Queue<T: DeserializeOwned> {
        #[pin]
        inner: Source,
        encoding: Encoding,
        has_closed: bool,
        _phantom: PhantomData<T>,
    }
}

impl<T: DeserializeOwned> Stream for Queue<T> {
    type Item = Result<Message<T>, QueueError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if *this.has_closed {
                return Poll::Ready(None);
            }

            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    let msg = msg.message;
                    let payload = match this.encoding {
                        #[cfg(feature = "json")]
                        Encoding::Json => serde_json::from_slice(&msg.payload)
                            .map_err(|err| QueueError::Json(err)),
                        #[cfg(feature = "msgpack")]
                        Encoding::MessagePack => rmp_serde::from_slice(&msg.payload)
                            .map_err(|err| QueueError::MessagePack(err)),
                    };

                    return Poll::Ready(Some(payload.map(|payload| Message {
                        subject: msg.subject,
                        reply: msg.reply,
                        payload,
                        headers: msg.headers,
                        status: msg.status,
                        description: msg.description,
                        length: msg.length,
                    })));
                }
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(QueueError::Nats(Box::new(err)))))
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
        }
    }
}

impl Intercom<HasEncoding> {
    pub async fn queue<S: ToString, T: DeserializeOwned>(
        &self,
        subject: S,
    ) -> Result<Queue<T>, async_nats::Error> {
        let subject = subject.to_string();

        let inner = self
            .jetstream
            .get_or_create_stream(StreamConfig {
                name: subject.replace(".", "|"),
                subjects: vec![subject.to_string()],
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        let consumer = inner.create_consumer(ConsumerConfig::default()).await?;

        Ok(Queue {
            inner: consumer.messages().await?,
            encoding: self.encoding.unwrap(),
            has_closed: false,
            _phantom: PhantomData,
        })
    }
}
