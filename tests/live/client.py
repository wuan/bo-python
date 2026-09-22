# -*- coding: utf8 -*-

"""JSON-RPC helpers for the live endpoint test suite.

The live service is deployed in several protocol variants depending on the
``txjsonrpc`` version and on the request shape:

* pre-v1 (``jsonrpclib.VERSION_PRE1``) responds with a bare JSON array,
  e.g. ``[{"result": ...}]``, ``[null]`` or ``[{"faultCode": ...}]``;
* v1 responds with ``{"result": ..., "error": null, "id": ...}``;
* v2 responds with ``{"jsonrpc": "2.0", "result": ..., "id": ...}``.

Both helpers here hide those differences so the limit assertions do not have
to care about the deployed dialect.
"""

import gzip
import json
from dataclasses import dataclass

import requests


def normalize_jsonrpc_response(payload, request_id):
    """Normalize any JSON-RPC response shape to a dict with a ``result`` key.

    A bare array (pre-v1) is unwrapped; ``dict`` responses are passed through
    after defaulting the ``jsonrpc``/``id`` fields; anything else is treated as
    a raw result value.
    """
    if isinstance(payload, list):
        result = payload[0] if payload else None
        return {"jsonrpc": "2.0", "id": request_id, "result": result}
    if isinstance(payload, dict):
        payload.setdefault("jsonrpc", "2.0")
        payload.setdefault("id", request_id)
        return payload
    return {"jsonrpc": "2.0", "id": request_id, "result": payload}


@dataclass(frozen=True)
class RawResponse:
    """Raw HTTP response with the body still compressed.

    ``requests`` transparently decompresses bodies, which is exactly what the
    format tests want but hides the transfer encoding.  The compression tests
    therefore use :meth:`JsonRpcClient.post_raw` and inspect this object.
    """

    status_code: int
    headers: dict
    body: bytes

    @property
    def content_encoding(self):
        """Return the lower-cased ``Content-Encoding`` header, or ``""``."""
        return (self.headers.get("Content-Encoding") or "").lower()

    def decoded_body(self):
        """Return the body with the declared content encoding applied."""
        if self.content_encoding == "gzip":
            return gzip.decompress(self.body)
        return self.body


class JsonRpcClient:
    """Minimal JSON-RPC client used to exercise the live endpoint.

    The headers that the endpoint inspects (``User-Agent``, ``Content-Type``
    and ``Accept-Encoding``) can be set independently per call so that the
    behavior matrix can be exercised without duplicating request construction.
    """

    def __init__(self, url: str, timeout: float = 15.0):
        self.url = url
        self.timeout = timeout
        self._session = requests.Session()
        self._request_id = 0

    def _build_request(
        self,
        method,
        params,
        user_agent,
        content_type,
        headers,
        *,
        request_id=None,
        version_field="2.0",
    ):
        if request_id is None:
            self._request_id += 1
            request_id = self._request_id
        request_headers = {}
        if user_agent is not None:
            request_headers["User-Agent"] = user_agent
        if content_type is not None:
            request_headers["Content-Type"] = content_type
        if headers:
            request_headers.update(headers)

        payload = {
            "method": method,
            "params": list(params) if params is not None else [],
            "id": request_id,
        }
        if version_field is not None:
            payload["jsonrpc"] = version_field
        return request_headers, payload

    def call_envelope(
        self,
        method,
        params=None,
        *,
        user_agent=None,
        content_type=None,
        headers=None,
        request_id=None,
        version_field="2.0",
    ):
        """Invoke ``method`` and return the raw, un-normalized JSON payload.

        Unlike :meth:`call`, the response envelope is returned as-is, so
        callers can assert on the actual protocol dialect (a pre-v1 bare array
        versus a v1/v2 object).

        ``version_field=None`` omits the ``jsonrpc`` member entirely and
        ``request_id=0`` models a legacy client that sends a fixed zero id.
        """
        request_headers, payload = self._build_request(
            method,
            params,
            user_agent,
            content_type,
            headers,
            request_id=request_id,
            version_field=version_field,
        )
        response = self._session.post(
            self.url,
            data=json.dumps(payload),
            headers=request_headers,
            timeout=self.timeout,
        )
        response.raise_for_status()

        if not response.content:
            return None
        return response.json()

    def call(self, method, params=None, *, user_agent=None, content_type=None, headers=None):
        """Invoke ``method`` and return the normalized JSON-RPC response.

        ``user_agent`` and ``content_type`` are only sent when not ``None``;
        passing ``None`` therefore models a request that omits the header
        entirely (``requests`` may still add its own default user agent).
        """
        request_headers, payload = self._build_request(
            method, params, user_agent, content_type, headers
        )

        response = self._session.post(
            self.url,
            data=json.dumps(payload),
            headers=request_headers,
            timeout=self.timeout,
        )
        response.raise_for_status()

        if not response.content:
            return {"jsonrpc": "2.0", "id": payload["id"], "result": None}

        try:
            return normalize_jsonrpc_response(response.json(), payload["id"])
        except ValueError:
            return {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": None,
                "raw": response.text,
            }

    def post_raw(self, method, params=None, *, user_agent=None, content_type=None, headers=None):
        """Invoke ``method`` and return the :class:`RawResponse`.

        Unlike :meth:`call`, the body is *not* decompressed, so callers can
        assert on the actual transfer encoding.
        """
        request_headers, payload = self._build_request(
            method, params, user_agent, content_type, headers
        )

        response = self._session.post(
            self.url,
            data=json.dumps(payload),
            headers=request_headers,
            timeout=self.timeout,
            stream=True,
        )
        try:
            body = response.raw.read(decode_content=False)
        finally:
            response.close()

        return RawResponse(response.status_code, response.headers, body)
