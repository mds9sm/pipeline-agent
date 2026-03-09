"""Credential encryption using Fernet symmetric encryption."""

import logging

from cryptography.fernet import Fernet, InvalidToken

log = logging.getLogger(__name__)


def generate_key() -> str:
    """Generate a new Fernet encryption key."""
    return Fernet.generate_key().decode()


def encrypt(plaintext: str, key: str) -> str:
    """Encrypt a string, return base64-encoded ciphertext."""
    if not key:
        raise ValueError("Encryption key is required")
    f = Fernet(key.encode() if isinstance(key, str) else key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str, key: str) -> str:
    """Decrypt base64-encoded ciphertext back to string."""
    if not key:
        raise ValueError("Encryption key is required")
    f = Fernet(key.encode() if isinstance(key, str) else key)
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        raise ValueError("Decryption failed — invalid key or corrupted data")


def encrypt_dict(data: dict, key: str, fields: list[str]) -> dict:
    """Encrypt specific fields in a dict, return copy with encrypted values."""
    result = dict(data)
    for field in fields:
        if field in result and result[field]:
            result[field] = encrypt(str(result[field]), key)
    return result


def decrypt_dict(data: dict, key: str, fields: list[str]) -> dict:
    """Decrypt specific fields in a dict, return copy with decrypted values."""
    result = dict(data)
    for field in fields:
        if field in result and result[field]:
            try:
                result[field] = decrypt(result[field], key)
            except ValueError:
                log.warning("Failed to decrypt field '%s' — may be plaintext", field)
    return result


CREDENTIAL_FIELDS = [
    "password", "source_password", "api_key", "secret", "token",
    "ssl_ca", "ssl_key", "ssl_cert",
]
