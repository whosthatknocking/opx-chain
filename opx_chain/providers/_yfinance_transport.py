"""Package-local verified Yahoo transport, independent of system CA defaults."""
from __future__ import annotations

from threading import Lock

import certifi
from curl_cffi import CurlOpt, ffi
from curl_cffi import requests as curl_requests

_SESSION: curl_requests.Session | None = None
_SESSION_LOCK = Lock()


def yfinance_session() -> curl_requests.Session:
    """Reuse one verified session and its cookies across Yahoo provider calls."""
    global _SESSION  # pylint: disable=global-statement
    with _SESSION_LOCK:
        if _SESSION is None:
            bundle = certifi.where()
            _SESSION = curl_requests.Session(
                impersonate="chrome",
                verify=True,
                curl_options={
                    CurlOpt.CAINFO: bundle,
                    CurlOpt.PROXY_CAINFO: bundle,
                    CurlOpt.CAPATH: ffi.NULL,
                    CurlOpt.PROXY_CAPATH: ffi.NULL,
                    CurlOpt.SSL_VERIFYPEER: 1,
                    CurlOpt.SSL_VERIFYHOST: 2,
                    CurlOpt.PROXY_SSL_VERIFYPEER: 1,
                    CurlOpt.PROXY_SSL_VERIFYHOST: 2,
                },
            )
        return _SESSION
