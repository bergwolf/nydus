// Copyright 2022 Ant Group. All rights reserved.
// Copyright (C) 2022 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

//! Base module used to implement object storage backend drivers (such as oss, s3, etc.).

use std::fmt;
use std::fmt::Debug;
use std::io::{Error, Result};
use std::marker::Send;
use std::sync::Arc;

use reqwest::header::{HeaderMap, CONTENT_LENGTH};
use reqwest::Method;

use nydus_utils::metrics::BackendMetrics;

use super::connection::{Connection, ConnectionError};
use super::{BackendError, BackendResult, BlobBackend, BlobReader};

/// Error codes related to object storage backend.
#[derive(Debug)]
pub enum ObjectStorageError {
    Auth(Error),
    Request(ConnectionError),
    ConstructHeader(String),
    Transport(reqwest::Error),
    Response(String),
}

impl fmt::Display for ObjectStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectStorageError::Auth(e) => write!(f, "failed to generate auth info, {}", e),
            ObjectStorageError::Request(e) => write!(f, "network communication error, {}", e),
            ObjectStorageError::ConstructHeader(e) => {
                write!(f, "failed to generate HTTP header, {}", e)
            }
            ObjectStorageError::Transport(e) => write!(f, "network communication error, {}", e),
            ObjectStorageError::Response(s) => write!(f, "network communication error, {}", s),
        }
    }
}

impl From<ObjectStorageError> for BackendError {
    fn from(err: ObjectStorageError) -> Self {
        BackendError::ObjectStorage(err)
    }
}

pub trait ObjectStorageState: Send + Sync + Debug {
    // `url` builds the resource path and full url for the object.
    fn url(&self, object_key: &str, query: &[&str]) -> (String, String);

    // `sign` signs the request with the access key and secret key.
    fn sign(
        &self,
        verb: Method,
        headers: &mut HeaderMap,
        canonicalized_resource: &str,
        full_resource_url: &str,
    ) -> Result<()>;

    fn retry_limit(&self) -> u8;
}

struct ObjectStorageReader<T>
where
    T: ObjectStorageState,
{
    blob_id: String,
    connection: Arc<Connection>,
    state: Arc<T>,
    metrics: Arc<BackendMetrics>,
}

impl<T> BlobReader for ObjectStorageReader<T>
where
    T: ObjectStorageState,
{
    fn blob_size(&self) -> BackendResult<u64> {
        let (resource, url) = self.state.url(&self.blob_id, &[]);
        let mut headers = HeaderMap::new();

        self.state
            .sign(Method::HEAD, &mut headers, resource.as_str(), url.as_str())
            .map_err(ObjectStorageError::Auth)?;

        let resp = self
            .connection
            .call::<&[u8]>(Method::HEAD, url.as_str(), None, None, &mut headers, true)
            .map_err(ObjectStorageError::Request)?;
        let content_length = resp
            .headers()
            .get(CONTENT_LENGTH)
            .ok_or_else(|| ObjectStorageError::Response("invalid content length".to_string()))?;

        Ok(content_length
            .to_str()
            .map_err(|err| {
                ObjectStorageError::Response(format!("invalid content length: {:?}", err))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ObjectStorageError::Response(format!("invalid content length: {:?}", err))
            })?)
    }

    fn try_read(&self, mut buf: &mut [u8], offset: u64) -> BackendResult<usize> {
        let query = &[];
        let (resource, url) = self.state.url(&self.blob_id, query);
        let mut headers = HeaderMap::new();
        let end_at = offset + buf.len() as u64 - 1;
        let range = format!("bytes={}-{}", offset, end_at);

        headers.insert(
            "Range",
            range
                .as_str()
                .parse()
                .map_err(|e| ObjectStorageError::ConstructHeader(format!("{}", e)))?,
        );
        self.state
            .sign(Method::GET, &mut headers, resource.as_str(), url.as_str())
            .map_err(ObjectStorageError::Auth)?;

        // Safe because the the call() is a synchronous operation.
        let mut resp = self
            .connection
            .call::<&[u8]>(Method::GET, url.as_str(), None, None, &mut headers, true)
            .map_err(ObjectStorageError::Request)?;
        Ok(resp
            .copy_to(&mut buf)
            .map_err(ObjectStorageError::Transport)
            .map(|size| size as usize)?)
    }

    fn metrics(&self) -> &BackendMetrics {
        &self.metrics
    }

    fn retry_limit(&self) -> u8 {
        self.state.retry_limit()
    }
}

#[derive(Debug)]
pub struct ObjectStorage<T>
where
    T: ObjectStorageState,
{
    connection: Arc<Connection>,
    state: Arc<T>,
    metrics: Option<Arc<BackendMetrics>>,
    #[allow(unused)]
    id: Option<String>,
}

impl<T> ObjectStorage<T>
where
    T: ObjectStorageState,
{
    pub(crate) fn new_object_storage(
        connection: Arc<Connection>,
        state: Arc<T>,
        metrics: Option<Arc<BackendMetrics>>,
        id: Option<String>,
    ) -> Self {
        ObjectStorage {
            connection,
            state,
            metrics,
            id,
        }
    }
}

impl<T: 'static> BlobBackend for ObjectStorage<T>
where
    T: ObjectStorageState,
{
    fn shutdown(&self) {
        self.connection.shutdown();
    }

    fn metrics(&self) -> &BackendMetrics {
        // `metrics()` is only used for nydusd, which will always provide valid `blob_id`, thus
        // `self.metrics` has valid value.
        self.metrics.as_ref().unwrap()
    }

    fn get_reader(&self, blob_id: &str) -> BackendResult<Arc<dyn BlobReader>> {
        if let Some(metrics) = self.metrics.as_ref() {
            Ok(Arc::new(ObjectStorageReader {
                blob_id: blob_id.to_string(),
                state: self.state.clone(),
                connection: self.connection.clone(),
                metrics: metrics.clone(),
            }))
        } else {
            Err(BackendError::Unsupported(
                "no metrics object available for OssReader".to_string(),
            ))
        }
    }
}

impl<T> Drop for ObjectStorage<T>
where
    T: ObjectStorageState,
{
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.release().unwrap_or_else(|e| error!("{:?}", e));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_object_storage_error_auth_display() {
        let err = ObjectStorageError::Auth(IoError::new(ErrorKind::PermissionDenied, "no creds"));
        let msg = format!("{}", err);
        assert!(msg.contains("failed to generate auth info"));
        assert!(msg.contains("no creds"));
    }

    #[test]
    fn test_object_storage_error_request_display() {
        let conn_err = ConnectionError::Disconnected;
        let err = ObjectStorageError::Request(conn_err);
        let msg = format!("{}", err);
        assert!(msg.contains("network communication error"));
        assert!(msg.contains("disconnected"));
    }

    #[test]
    fn test_object_storage_error_construct_header_display() {
        let err = ObjectStorageError::ConstructHeader("bad header value".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("failed to generate HTTP header"));
        assert!(msg.contains("bad header value"));
    }

    #[test]
    fn test_object_storage_error_response_display() {
        let err = ObjectStorageError::Response("404 not found".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("network communication error"));
        assert!(msg.contains("404 not found"));
    }

    #[test]
    fn test_from_object_storage_error_to_backend_error() {
        let err = ObjectStorageError::ConstructHeader("test".to_string());
        let backend_err: BackendError = err.into();
        match backend_err {
            BackendError::ObjectStorage(_) => {} // expected
            _ => panic!("Expected BackendError::ObjectStorage"),
        }
    }

    #[test]
    fn test_object_storage_error_debug() {
        let err = ObjectStorageError::Response("test debug".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Response"));
        assert!(debug_str.contains("test debug"));
    }

    #[derive(Debug)]
    struct MockObjectStorageState {
        retry: u8,
    }

    impl ObjectStorageState for MockObjectStorageState {
        fn url(&self, object_key: &str, _query: &[&str]) -> (String, String) {
            (
                format!("/{}", object_key),
                format!("https://example.com/{}", object_key),
            )
        }

        fn sign(
            &self,
            _verb: Method,
            _headers: &mut HeaderMap,
            _canonicalized_resource: &str,
            _full_resource_url: &str,
        ) -> Result<()> {
            Ok(())
        }

        fn retry_limit(&self) -> u8 {
            self.retry
        }
    }

    #[test]
    fn test_new_object_storage_construction() {
        use super::super::connection::ConnectionConfig;

        let config = ConnectionConfig::default();
        let connection = Connection::new(&config).unwrap();
        let state = Arc::new(MockObjectStorageState { retry: 3 });
        let metrics = BackendMetrics::new("test-obj", "test-instance");

        let storage =
            ObjectStorage::new_object_storage(connection, state, Some(metrics), Some("id1".into()));

        assert!(storage.metrics.is_some());
        assert_eq!(storage.id, Some("id1".to_string()));
    }

    #[test]
    fn test_new_object_storage_no_metrics() {
        use super::super::connection::ConnectionConfig;

        let config = ConnectionConfig::default();
        let connection = Connection::new(&config).unwrap();
        let state = Arc::new(MockObjectStorageState { retry: 0 });

        let storage: ObjectStorage<MockObjectStorageState> =
            ObjectStorage::new_object_storage(connection, state, None, None);

        assert!(storage.metrics.is_none());
        assert_eq!(storage.id, None);
    }

    #[test]
    fn test_get_reader_without_metrics_returns_error() {
        use super::super::connection::ConnectionConfig;

        let config = ConnectionConfig::default();
        let connection = Connection::new(&config).unwrap();
        let state = Arc::new(MockObjectStorageState { retry: 0 });

        let storage: ObjectStorage<MockObjectStorageState> =
            ObjectStorage::new_object_storage(connection, state, None, None);

        let result = storage.get_reader("some-blob");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_reader_with_metrics_returns_ok() {
        use super::super::connection::ConnectionConfig;

        let config = ConnectionConfig::default();
        let connection = Connection::new(&config).unwrap();
        let state = Arc::new(MockObjectStorageState { retry: 2 });
        let metrics = BackendMetrics::new("test-reader", "test-reader-instance");

        let storage =
            ObjectStorage::new_object_storage(connection, state, Some(metrics), None);

        let reader = storage.get_reader("my-blob").unwrap();
        assert_eq!(reader.retry_limit(), 2);
    }
}
