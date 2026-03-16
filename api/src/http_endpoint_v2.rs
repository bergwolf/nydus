// Copyright 2022 Alibaba Cloud. All rights reserved.
// Copyright 2020 Ant Group. All rights reserved.
// Copyright © 2019 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! Nydus API v2.

use crate::BlobCacheEntry;
use dbs_uhttp::{Method, Request, Response};

use crate::http::{
    ApiError, ApiRequest, ApiResponse, ApiResponsePayload, BlobCacheObjectId, HttpError,
};
use crate::http_handler::{
    error_response, extract_query_part, parse_body, success_response, translate_status_code,
    EndpointHandler, HttpResult,
};

/// HTTP URI prefix for API v2.
pub const HTTP_ROOT_V2: &str = "/api/v2";

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
                BlobObjectList(d) => success_response(Some(d)),
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
pub struct InfoV2Handler {}
impl EndpointHandler for InfoV2Handler {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                let r = kicker(ApiRequest::GetDaemonInfoV2);
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

/// List blob objects managed by the blob cache manager.
pub struct BlobObjectListHandlerV2 {}
impl EndpointHandler for BlobObjectListHandlerV2 {
    fn handle_request(
        &self,
        req: &Request,
        kicker: &dyn Fn(ApiRequest) -> ApiResponse,
    ) -> HttpResult {
        match (req.method(), req.body.as_ref()) {
            (Method::Get, None) => {
                if let Some(domain_id) = extract_query_part(req, "domain_id") {
                    let blob_id = extract_query_part(req, "blob_id").unwrap_or_default();
                    let param = BlobCacheObjectId { domain_id, blob_id };
                    let r = kicker(ApiRequest::GetBlobObject(param));
                    return Ok(convert_to_response(r, HttpError::GetBlobObjects));
                }
                Err(HttpError::BadRequest)
            }
            (Method::Put, Some(body)) => {
                let mut conf: Box<BlobCacheEntry> = parse_body(body)?;
                if !conf.prepare_configuration_info() {
                    return Err(HttpError::BadRequest);
                }
                let r = kicker(ApiRequest::CreateBlobObject(conf));
                Ok(convert_to_response(r, HttpError::CreateBlobObject))
            }
            (Method::Delete, None) => {
                if let Some(domain_id) = extract_query_part(req, "domain_id") {
                    let blob_id = extract_query_part(req, "blob_id").unwrap_or_default();
                    let param = BlobCacheObjectId { domain_id, blob_id };
                    let r = kicker(ApiRequest::DeleteBlobObject(param));
                    return Ok(convert_to_response(r, HttpError::DeleteBlobObject));
                }
                if let Some(blob_id) = extract_query_part(req, "blob_id") {
                    let r = kicker(ApiRequest::DeleteBlobFile(blob_id));
                    return Ok(convert_to_response(r, HttpError::DeleteBlobFile));
                }
                Err(HttpError::BadRequest)
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
    fn test_http_root_v2() {
        assert_eq!(HTTP_ROOT_V2, "/api/v2");
    }

    #[test]
    fn test_info_v2_handler_get() {
        let handler = InfoV2Handler {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::DaemonInfo("{}".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v2/daemon HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_info_v2_handler_put() {
        let handler = InfoV2Handler {};
        let body = r#"{"log_level":"info"}"#;
        let raw = format!(
            "PUT http://localhost/api/v2/daemon HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_info_v2_handler_bad_method() {
        let handler = InfoV2Handler {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v2/daemon HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_blob_object_list_handler_get_with_domain_id() {
        let handler = BlobObjectListHandlerV2 {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BlobObjectList("[]".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v2/blobs?domain_id=dom1 HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_blob_object_list_handler_get_with_domain_and_blob_id() {
        let handler = BlobObjectListHandlerV2 {};
        let kicker = |_req: ApiRequest| -> ApiResponse {
            Ok(ApiResponsePayload::BlobObjectList("[]".to_string()))
        };
        let req = Request::try_from(
            b"GET http://localhost/api/v2/blobs?domain_id=dom1&blob_id=blob1 HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_blob_object_list_handler_get_missing_domain_id() {
        let handler = BlobObjectListHandlerV2 {};
        let req = Request::try_from(
            b"GET http://localhost/api/v2/blobs HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_blob_object_list_handler_delete_with_domain_id() {
        let handler = BlobObjectListHandlerV2 {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v2/blobs?domain_id=dom1 HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_blob_object_list_handler_delete_with_blob_id() {
        let handler = BlobObjectListHandlerV2 {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v2/blobs?blob_id=blob1 HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        let resp = handler.handle_request(&req, &ok_kicker).unwrap();
        assert_eq!(resp.status(), StatusCode::NoContent);
    }

    #[test]
    fn test_blob_object_list_handler_delete_no_params() {
        let handler = BlobObjectListHandlerV2 {};
        let req = Request::try_from(
            b"DELETE http://localhost/api/v2/blobs HTTP/1.0\r\n\r\n",
            None,
        )
        .unwrap();
        assert!(handler.handle_request(&req, &ok_kicker).is_err());
    }

    #[test]
    fn test_blob_object_list_handler_put_invalid_body() {
        let handler = BlobObjectListHandlerV2 {};
        let body = r#"{"type":"bootstrap","id":"blob1","config_v2":{"version":2},"domain_id":"dom1"}"#;
        let raw = format!(
            "PUT http://localhost/api/v2/blobs HTTP/1.0\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            body.len(),
            body
        );
        let req = Request::try_from(raw.as_bytes(), None).unwrap();
        // prepare_configuration_info may return false for minimal config, resulting in BadRequest
        let result = handler.handle_request(&req, &ok_kicker);
        // The result can be either Ok (if prepare succeeds) or Err (BadRequest if it fails).
        // With minimal config_v2, prepare_configuration_info should fail.
        assert!(result.is_err());
    }

    #[test]
    fn test_blob_object_list_handler_bad_method() {
        let handler = BlobObjectListHandlerV2 {};
        let req = Request::try_from(
            b"POST http://localhost/api/v2/blobs HTTP/1.0\r\n\r\n",
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
    fn test_convert_to_response_blob_object_list() {
        let resp = convert_to_response(
            Ok(ApiResponsePayload::BlobObjectList("[]".to_string())),
            HttpError::GetBlobObjects,
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
