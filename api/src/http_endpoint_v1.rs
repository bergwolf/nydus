// Copyright 2020 Ant Group. All rights reserved.
// Copyright © 2019 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! Nydus API v1.

use dbs_uhttp::{Method, Request, Response};

use crate::http::{ApiError, ApiRequest, ApiResponse, ApiResponsePayload, Config, HttpError};
use crate::http_handler::{
    error_response, extract_query_part, parse_body, success_response, translate_status_code,
    EndpointHandler, HttpResult,
};

/// HTTP URI prefix for API v1.
pub const HTTP_ROOT_V1: &str = "/api/v1";

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
                DaemonInfo(d) => success_response(Some(d)),
                FsGlobalMetrics(d) => success_response(Some(d)),
                FsFilesMetrics(d) => success_response(Some(d)),
                FsFilesPatterns(d) => success_response(Some(d)),
                FsBackendInfo(d) => success_response(Some(d)),
                FsInflightMetrics(d) => success_response(Some(d)),
                Config(conf) => {
                    let json = serde_json::to_string(&conf).unwrap_or_else(|_| "{}".to_string());
                    success_response(Some(json))
                }
                _ => panic!("Unexpected response message from API service"),
            }
        }
        Err(e) => {
            let status_code = translate_status_code(&e);
            error_response(op(e), status_code)
        }
    }
}

/// Get daemon information and set daemon configuration.
pub struct InfoHandler {}
impl EndpointHandler for InfoHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let r = kicker(ApiRequest::GetDaemonInfo);
                Ok(convert_to_response(r, HttpError::DaemonInfo))
            }
            (Method::Put, Some(body)) => {
                let conf = parse_body(body)?;
                let r = kicker(ApiRequest::ConfigureDaemon(conf));
                Ok(convert_to_response(r, HttpError::Configure))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get filesystem backend information.
pub struct FsBackendInfo {}
impl EndpointHandler for FsBackendInfo {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let mountpoint = extract_query_part(req, "mountpoint").ok_or_else(|| {
                    HttpError::QueryString(
                        "'mountpoint' should be specified in query string".to_string(),
                    )
                })?;
                let r = kicker(ApiRequest::ExportFsBackendInfo(mountpoint));
                Ok(convert_to_response(r, HttpError::FsBackendInfo))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get filesystem global metrics.
pub struct MetricsFsGlobalHandler {}
impl EndpointHandler for MetricsFsGlobalHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::ExportFsGlobalMetrics(id));
                Ok(convert_to_response(r, HttpError::GlobalMetrics))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get filesystem access pattern log.
pub struct MetricsFsAccessPatternHandler {}
impl EndpointHandler for MetricsFsAccessPatternHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::ExportFsAccessPatterns(id));
                Ok(convert_to_response(r, HttpError::Pattern))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get filesystem file metrics.
pub struct MetricsFsFilesHandler {}
impl EndpointHandler for MetricsFsFilesHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let latest_read_files = extract_query_part(req, "latest")
                    .is_some_and(|b| b.parse::<bool>().unwrap_or(false));
                let r = kicker(ApiRequest::ExportFsFilesMetrics(id, latest_read_files));
                Ok(convert_to_response(r, HttpError::FsFilesMetrics))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Get information about filesystem inflight requests.
pub struct MetricsFsInflightHandler {}
impl EndpointHandler for MetricsFsInflightHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let r = kicker(ApiRequest::ExportFsInflightMetrics);
                Ok(convert_to_response(r, HttpError::InflightMetrics))
            }
            _ => Err(HttpError::BadRequest),
        }
    }
}

/// Update global configuration of the daemon.
pub struct ConfigHandler {}
impl EndpointHandler for ConfigHandler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::GetConfig(id));
                Ok(convert_to_response(r, HttpError::Configure))
            }
            (Method::Put, Some(body)) => {
                let conf: Config = parse_body(body)?;
                let id = extract_query_part(req, "id");
                let r = kicker(ApiRequest::UpdateConfig(id, conf));
                Ok(convert_to_response(r, HttpError::Configure))
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
    fn test_http_root_v1() {
        assert_eq!(HTTP_ROOT_V1, "/api/v1");
    }

    #[test]
    fn test_info_handler_get() {
        let handler = InfoHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::DaemonInfo("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_info_handler_put() {
        let handler = InfoHandler {};
        let body = r#"{"log_level":"debug"}"#;
        let raw = format!(
            "PUT http://localhost/api/v1/daemon HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_info_handler_bad_method() {
        let handler = InfoHandler {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v1/daemon HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_fs_backend_info_get() {
        let handler = FsBackendInfo {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsBackendInfo("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/backend?mountpoint=/mnt HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_fs_backend_info_missing_mountpoint() {
        let handler = FsBackendInfo {};
        let req = Request::try_from(
            b"GET http://localhost/api/v1/daemon/backend HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let result = handler.handle_request(&req, &ok_kicker);
        assert!(result.is_err());
    }

    #[test]
    fn test_fs_backend_info_bad_method() {
        let handler = FsBackendInfo {};
        let req = Request::try_from(
            b"PUT http://localhost/api/v1/daemon/backend HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_fs_global_handler_get() {
        let handler = MetricsFsGlobalHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsGlobalMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_global_handler_get_with_id() {
        let handler = MetricsFsGlobalHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsGlobalMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics?id=test HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_global_handler_bad_method() {
        let handler = MetricsFsGlobalHandler {};
        let req = Request::try_from(
            b"POST http://localhost/api/v1/metrics HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_fs_access_pattern_handler_get() {
        let handler = MetricsFsAccessPatternHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsFilesPatterns("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/pattern HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_access_pattern_handler_bad_method() {
        let handler = MetricsFsAccessPatternHandler {};
        let req = Request::try_from(
            b"POST http://localhost/api/v1/metrics/pattern HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_fs_files_handler_get() {
        let handler = MetricsFsFilesHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsFilesMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/files HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_files_handler_get_with_params() {
        let handler = MetricsFsFilesHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsFilesMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/files?id=test&latest=true HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_files_handler_bad_method() {
        let handler = MetricsFsFilesHandler {};
        let req = Request::try_from(
            b"POST http://localhost/api/v1/metrics/files HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_metrics_fs_inflight_handler_get() {
        let handler = MetricsFsInflightHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::FsInflightMetrics("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/metrics/inflight HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_metrics_fs_inflight_handler_bad_method() {
        let handler = MetricsFsInflightHandler {};
        let req = Request::try_from(
            b"POST http://localhost/api/v1/metrics/inflight HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_config_handler_get() {
        let handler = ConfigHandler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::Config(std::collections::HashMap::new()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v1/config HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_config_handler_put() {
        let handler = ConfigHandler {};
        let body = r#"{"key1":"value1"}"#;
        let raw = format!(
            "PUT http://localhost/api/v1/config HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_config_handler_bad_method() {
        let handler = ConfigHandler {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v1/config HTTP/1.0\r\n\r\n",
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
    fn test_convert_to_response_daemon_info() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::DaemonInfo("{}".to_string())),
            HttpError::DaemonInfo,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_fs_global_metrics() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::FsGlobalMetrics("{}".to_string())),
            HttpError::GlobalMetrics,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_fs_files_metrics() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::FsFilesMetrics("{}".to_string())),
            HttpError::FsFilesMetrics,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_fs_files_patterns() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::FsFilesPatterns("{}".to_string())),
            HttpError::Pattern,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_fs_backend_info() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::FsBackendInfo("{}".to_string())),
            HttpError::FsBackendInfo,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_fs_inflight_metrics() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::FsInflightMetrics("{}".to_string())),
            HttpError::InflightMetrics,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_config() {
        let mut config = std::collections::HashMap::new();
        config.insert("key".to_string(), "value".to_string());
        let resp = convert_to_response(
            Ok(ApiResponsePayload::Config(config)),
            HttpError::Configure,
        );
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_convert_to_response_error() {
        let api_resp: ApiResponse = Err(ApiError::ResponsePayloadType);
        let resp = convert_to_response(api_resp, HttpError::DaemonInfo);
        assert_eq!(resp.status(), StatusCode::InternalServerError);
    }
}
