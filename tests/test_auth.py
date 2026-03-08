"""Tests for auth.py -- JWT authentication and FastAPI dependency."""

from __future__ import annotations

import os
import sys
import time
from unittest.mock import MagicMock, AsyncMock

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import jwt as pyjwt
from fastapi import HTTPException

from auth import create_token, verify_token, AuthDependency

pytestmark = pytest.mark.asyncio


SECRET = "test-jwt-secret-key-1234567890"
ALGORITHM = "HS256"


# ======================================================================
# create_token / verify_token
# ======================================================================


class TestTokenCreateVerify:

    def test_create_and_verify_token(self):
        """Create token, verify returns correct sub and role."""
        token = create_token(
            user_id="user-42",
            secret=SECRET,
            algorithm=ALGORITHM,
            expiry_hours=1,
            role="admin",
        )
        assert isinstance(token, str)

        payload = verify_token(token, SECRET, ALGORITHM)
        assert payload["sub"] == "user-42"
        assert payload["role"] == "admin"
        assert "exp" in payload
        assert "iat" in payload

    def test_expired_token(self):
        """Token with past expiry raises 401."""
        # Create a token that already expired
        expired_payload = {
            "sub": "user-expired",
            "role": "viewer",
            "iat": int(time.time()) - 7200,
            "exp": int(time.time()) - 3600,  # expired 1 hour ago
        }
        token = pyjwt.encode(expired_payload, SECRET, algorithm=ALGORITHM)

        with pytest.raises(HTTPException) as exc_info:
            verify_token(token, SECRET, ALGORITHM)
        assert exc_info.value.status_code == 401
        assert "expired" in exc_info.value.detail.lower()

    def test_invalid_token(self):
        """Garbage token raises 401."""
        with pytest.raises(HTTPException) as exc_info:
            verify_token("not.a.real.token", SECRET, ALGORITHM)
        assert exc_info.value.status_code == 401
        assert "invalid" in exc_info.value.detail.lower()


# ======================================================================
# AuthDependency
# ======================================================================


class _FakeHeaders(dict):
    """Dict subclass that behaves like Starlette Headers."""
    pass


def _make_request(headers: dict | None = None) -> MagicMock:
    """Build a mock FastAPI Request with given headers."""
    request = MagicMock()
    _headers = _FakeHeaders(headers or {})
    request.headers = _headers
    return request


class TestAuthDependencyDisabled:

    async def test_auth_dependency_disabled(self):
        """When auth_enabled=False, returns anonymous admin."""
        config = MagicMock()
        config.auth_enabled = False

        dep = AuthDependency(config)
        request = _make_request()

        result = await dep(request, credentials=None)
        assert result["sub"] == "anonymous"
        assert result["role"] == "admin"


class TestAuthDependencyEnabled:

    async def test_auth_dependency_bearer(self):
        """When auth_enabled=True, valid bearer token works."""
        config = MagicMock()
        config.auth_enabled = True
        config.jwt_secret = SECRET
        config.jwt_algorithm = ALGORITHM

        token = create_token(
            user_id="bearer-user",
            secret=SECRET,
            algorithm=ALGORITHM,
            role="editor",
        )

        dep = AuthDependency(config)
        request = _make_request(headers={
            "Authorization": f"Bearer {token}",
        })

        result = await dep(request, credentials=None)
        assert result["sub"] == "bearer-user"
        assert result["role"] == "editor"

    async def test_auth_dependency_api_key(self):
        """When auth_enabled=True, valid X-API-Key works."""
        config = MagicMock()
        config.auth_enabled = True
        config.jwt_secret = SECRET
        config.jwt_algorithm = ALGORITHM

        dep = AuthDependency(config)
        request = _make_request(headers={
            "X-API-Key": SECRET,  # API key matches jwt_secret
        })

        result = await dep(request, credentials=None)
        assert result["sub"] == "api_key_user"
        assert result["role"] == "admin"

    async def test_auth_dependency_no_credentials(self):
        """When auth_enabled=True, no credentials raises 401."""
        config = MagicMock()
        config.auth_enabled = True
        config.jwt_secret = SECRET
        config.jwt_algorithm = ALGORITHM

        dep = AuthDependency(config)
        request = _make_request(headers={})

        with pytest.raises(HTTPException) as exc_info:
            await dep(request, credentials=None)
        assert exc_info.value.status_code == 401
        assert "authentication" in exc_info.value.detail.lower()
