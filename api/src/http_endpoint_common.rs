// Copyright 2020 Ant Group. All rights reserved.
// Copyright © 2019 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

use dbs_uhttp::{Method, Request, Response};

use crate::http::{ApiError, ApiRequest, ApiResponse, ApiResponsePayload, HttpError};
use crate::http_handler::{
    error_response, extract_query_part, parse_body, success_response, translate_status_code,
    EndpointHandler, HttpResult,
};

// Convert an ApiResponse to a HTTP response.
//
// API server has successfully processed the request, but can't fulfill that. Therefore,
// a `error_response` is generated whose status code is 4XX or 5XX. With error response,
// it still returns Ok(error_response) to http request handling framework, which means
// nydusd api server receives the request and try handle it, even the request can't be fulfilled.
fn convert_to_response<O: FnOnce(ApiError) -> HttpError>(api_resp: ApiResponse, op: O) -> Response {
    match api_resp {
        Ok(r) => {
            use ApiResponsePayload::*;
            match r {
                Empty => success_response(None),
                Events(d) => success_response(Some(d)),
                BackendMetrics(d) => success_response(Some(d)),
                BlobcacheMetrics(d) => success_response(Some(d)),
                _ => panic!("Unexpected response message from API service"),
            }
        }
        Err(e) => {
            let status_code = translate_status_code(&e);
            error_response(op(e), status_code)
        }
    }
}
// Global daemon control requests.
/// Start the daemon.
pub struct StartHandler {}
impl EndpointHandler for StartHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Put, None) => {
                let r = kicker(ApiRequest::Start);
                Ok(convert_to_response(r, HttpError::Configure))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Stop the daemon.
pub struct ExitHandler {}
impl EndpointHandler for ExitHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Put, None) => {
                let r = kicker(ApiRequest::Exit);
                Ok(convert_to_response(r, HttpError::Configure))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get daemon global events.
pub struct EventsHandler {}
impl EndpointHandler for EventsHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let r = kicker(ApiRequest::GetEvents);
                Ok(convert_to_response(r, HttpError::Events))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

// Metrics related requests.
/// Get storage backend metrics.
pub struct MetricsBackendHandler {}
impl EndpointHandler for MetricsBackendHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::ExportBackendMetrics(id));
                Ok(convert_to_response(r, HttpError::BackendMetrics))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get blob cache metrics.
pub struct MetricsBlobcacheHandler {}
impl EndpointHandler for MetricsBlobcacheHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::ExportBlobcacheMetrics(id));
                Ok(convert_to_response(r, HttpError::BlobcacheMetrics))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Mount a filesystem.
pub struct MountHandler {}
impl EndpointHandler for MountHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        let mountpoint = extract_query_part(req, "mountpoint").ok_or_else(|| {
            HttpError::QueryString("'mountpoint' should be specified in query string".to_string())
        })?;
        match (req.method(), req.body.as_ref()) {
            (Method::Post, Some(body)) => {
                let cmd = parse_body(body)?;
                let r = kicker(ApiRequest::Mount(mountpoint, cmd));
                Ok(convert_to_response(r, HttpError::Mount))
            }
            (Method::Put, Some(body)) => {
                let cmd = parse_body(body)?;
                let r = kicker(ApiRequest::Remount(mountpoint, cmd));
                Ok(convert_to_response(r, HttpError::Mount))
            }
            (Method::Delete, None) => {
                let r = kicker(ApiRequest::Umount(mountpoint));
                Ok(convert_to_response(r, HttpError::Mount))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Send fuse fd to new daemon.
pub struct SendFuseFdHandler {}
impl EndpointHandler for SendFuseFdHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Put, None) => {
                let r = kicker(ApiRequest::SendFuseFd);
                Ok(convert_to_response(r, HttpError::Upgrade))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Take over fuse fd from old daemon instance.
pub struct TakeoverFuseFdHandler {}
impl EndpointHandler for TakeoverFuseFdHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Put, None) => {
                let r = kicker(ApiRequest::TakeoverFuseFd);
                Ok(convert_to_response(r, HttpError::Upgrade))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbs_uhttp::StatusCode;

    fn ok_kicker(_req: ApiRequest) -> ApiResponse {
        Ok(ApiResponsePayload::Empty)
    }

    #[test]
    fn test_start_handler_put() {
        let handler = StartHandler {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/start HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_start_handler_bad_method() {
        let handler = StartHandler {};
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/start HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let result = handler.handle_request(&req, &ok_kicker);
        assert!(result.is_err());
    }

    #[test]
    fn test_exit_handler_put() {
        let handler = ExitHandler {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/exit HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_exit_handler_bad_method() {
        let handler = ExitHandler {};
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/exit HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_events_handler_get() {
        let handler = EventsHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::Events("[]".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/events HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_events_handler_bad_method() {
        let handler = EventsHandler {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/events HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_backend_handler_get() {
        let handler = MetricsBackendHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BackendMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/backend HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_backend_handler_get_with_id() {
        let handler = MetricsBackendHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BackendMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/backend?id=test HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_backend_handler_bad_method() {
        let handler = MetricsBackendHandler {};
        let req = Request::try_from(
            b"POST http://localhost/api/v1/metrics/backend HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_blobcache_handler_get() {
        let handler = MetricsBlobcacheHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BlobcacheMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/blobcache HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_blobcache_handler_get_with_id() {
        let handler = MetricsBlobcacheHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BlobcacheMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/blobcache?id=test HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_mount_handler_post() {
        let handler = MountHandler {};
        let body = r#"{"source":"/src","config":"{}"}"#;
        let raw = format!(
            "POST http://localhost/api/v1/mount?mountpoint=/mnt HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_mount_handler_put() {
        let handler = MountHandler {};
        let body = r#"{"source":"/src","config":"{}"}"#;
        let raw = format!(
            "PUT http://localhost/api/v1/mount?mountpoint=/mnt HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_mount_handler_delete() {
        let handler = MountHandler {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v1/mount?mountpoint=/mnt HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_mount_handler_missing_mountpoint() {
        let handler = MountHandler {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v1/mount HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let result = handler.handle_request(&req, &ok_kicker);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_fuse_fd_handler_put() {
        let handler = SendFuseFdHandler {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/fuse/sendfd HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_send_fuse_fd_handler_bad_method() {
        let handler = SendFuseFdHandler {};
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/fuse/sendfd HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_takeover_fuse_fd_handler_put() {
        let handler = TakeoverFuseFdHandler {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/fuse/takeover HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_takeover_fuse_fd_handler_bad_method() {
        let handler = TakeoverFuseFdHandler {};
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/fuse/takeover HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_convert_to_response_empty() {
        let resp = convert_to_response(Ok(ApiResponsePayload::Empty), HttpError::Configure);
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_convert_to_response_events() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::Events("[]".to_string())),
            HttpError::Events,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_backend_metrics() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::BackendMetrics("{}".to_string())),
            HttpError::BackendMetrics,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_blobcache_metrics() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::BlobcacheMetrics("{}".to_string())),
            HttpError::BlobcacheMetrics,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_error() {
        let api_resp: ApiResponse = Err(ApiError::ResponsePayloadType);
        let resp = convert_to_response(api_resp, HttpError::Configure);
        assert_eq!(resp.status(), StatusCode::InternalServerError);
    }
}
