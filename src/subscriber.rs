use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use async_nats::subject::ToSubject;
use futures::Stream;
use pin_project_lite::pin_project;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{nats, Encoder, Encoding, Intercom, Message};

#[derive(Debug, Error)]
pub enum SubscriberError {
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
    pub struct Subscriber<T: DeserializeOwned> {
        #[pin]
        inner: nats::Subscriber,
        encoding: Encoding,
        has_closed: bool,
        _phantom: PhantomData<T>,
    }
}

impl<T: DeserializeOwned> Stream for Subscriber<T> {
    type Item = Result<Message<T>, SubscriberError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if *this.has_closed {
                return Poll::Ready(None);
            }

            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(msg)) => {
                    let payload = match this.encoding {
                        Encoding::NoEncoding => unreachable!(),
                        #[cfg(feature = "json")]
                        Encoding::Json => serde_json::from_slice(&msg.payload)
                            .map_err(|err| SubscriberError::Json(err)),
                        #[cfg(feature = "msgpack")]
                        Encoding::MessagePack => rmp_serde::from_slice(&msg.payload)
                            .map_err(|err| SubscriberError::MessagePack(err)),
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
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
        }
    }
}

impl<E: Encoder> Intercom<E> {
    pub async fn subscribe<S: ToSubject, T: DeserializeOwned>(
        &self,
        subject: S,
    ) -> Result<Subscriber<T>, async_nats::Error> {
        let inner = self.inner.subscribe(subject).await?;
        Ok(Subscriber {
            inner,
            encoding: self.encoding,
            has_closed: false,
            _phantom: PhantomData,
        })
    }

    pub async fn queue_subscribe<S: ToSubject, Q: ToString, T: DeserializeOwned>(
        &self,
        subject: S,
        group: Q,
    ) -> Result<Subscriber<T>, async_nats::Error> {
        let inner = self
            .inner
            .queue_subscribe(subject, group.to_string())
            .await?;
        Ok(Subscriber {
            inner,
            encoding: self.encoding,
            has_closed: false,
            _phantom: PhantomData,
        })
    }
}
