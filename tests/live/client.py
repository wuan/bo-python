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

import json

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


class JsonRpcClient:
    """Minimal JSON-RPC client used to exercise the live endpoint.

    The two headers that the endpoint inspects (``User-Agent`` and
    ``Content-Type``) can be set independently per call so that the limit
    matrix can be exercised without duplicating request construction.
    """

    def __init__(self, url: str, timeout: float = 15.0):
        self.url = url
        self.timeout = timeout
        self._session = requests.Session()
        self._request_id = 0

    def call(self, method, params=None, *, user_agent=None, content_type=None, headers=None):
        """Invoke ``method`` and return the normalized JSON-RPC response.

        ``user_agent`` and ``content_type`` are only sent when not ``None``;
        passing ``None`` therefore models a request that omits the header
        entirely (``requests`` may still add its own default user agent).
        """
        self._request_id += 1
        request_headers = {}
        if user_agent is not None:
            request_headers["User-Agent"] = user_agent
        if content_type is not None:
            request_headers["Content-Type"] = content_type
        if headers:
            request_headers.update(headers)

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": list(params) if params is not None else [],
            "id": self._request_id,
        }

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
