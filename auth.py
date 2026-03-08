"""JWT authentication for the API layer."""

import logging
import time
from typing import Optional

import jwt
from fastapi import Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

log = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


def create_token(
    user_id: str,
    secret: str,
    algorithm: str = "HS256",
    expiry_hours: int = 24,
    role: str = "viewer",
) -> str:
    payload = {
        "sub": user_id,
        "role": role,
        "iat": int(time.time()),
        "exp": int(time.time()) + expiry_hours * 3600,
    }
    return jwt.encode(payload, secret, algorithm=algorithm)


def verify_token(token: str, secret: str, algorithm: str = "HS256") -> dict:
    try:
        return jwt.decode(token, secret, algorithms=[algorithm])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


class AuthDependency:
    """FastAPI dependency for JWT auth. Bypassed when auth_enabled=False."""

    def __init__(self, config):
        self.config = config

    async def __call__(self, request: Request) -> dict:
        if not self.config.auth_enabled:
            return {"sub": "anonymous", "role": "admin"}

        # Check Bearer token
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return verify_token(
                token, self.config.jwt_secret, self.config.jwt_algorithm
            )

        # Check X-API-Key header
        api_key = request.headers.get("X-API-Key", "")
        if api_key and self.config.jwt_secret and api_key == self.config.jwt_secret:
            return {"sub": "api_key_user", "role": "admin"}

        raise HTTPException(status_code=401, detail="Authentication required")
