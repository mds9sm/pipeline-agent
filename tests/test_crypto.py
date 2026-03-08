"""Tests for crypto.py -- Fernet encryption utilities."""

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from crypto import encrypt, decrypt, encrypt_dict, decrypt_dict, generate_key


class TestEncryptDecrypt:
    """Core encrypt / decrypt round-trip and error handling."""

    def test_encrypt_decrypt_roundtrip(self):
        key = generate_key()
        plaintext = "super-secret-password-123!"
        ciphertext = encrypt(plaintext, key)
        assert ciphertext != plaintext
        assert decrypt(ciphertext, key) == plaintext

    def test_encrypt_requires_key(self):
        with pytest.raises(ValueError, match="Encryption key is required"):
            encrypt("hello", "")

    def test_decrypt_wrong_key(self):
        key1 = generate_key()
        key2 = generate_key()
        ciphertext = encrypt("secret", key1)
        with pytest.raises(ValueError, match="invalid key or corrupted"):
            decrypt(ciphertext, key2)

    def test_encrypt_empty_string(self):
        key = generate_key()
        ciphertext = encrypt("", key)
        assert decrypt(ciphertext, key) == ""


class TestEncryptDict:
    """Selective field encryption / decryption on dicts."""

    def test_encrypt_dict_selective(self):
        key = generate_key()
        data = {"host": "localhost", "password": "s3cret", "port": 5432}
        encrypted = encrypt_dict(data, key, ["password"])
        # password should be encrypted (not original value)
        assert encrypted["password"] != "s3cret"
        # host should be untouched
        assert encrypted["host"] == "localhost"
        # port should be untouched
        assert encrypted["port"] == 5432
        # round-trip
        decrypted = decrypt_dict(encrypted, key, ["password"])
        assert decrypted["password"] == "s3cret"

    def test_decrypt_dict_tolerates_plaintext(self):
        """decrypt_dict should not crash on fields that are not encrypted."""
        key = generate_key()
        data = {"host": "localhost", "password": "plaintext-value"}
        # password is plaintext, not Fernet-encrypted -- should log warning, not crash
        result = decrypt_dict(data, key, ["password"])
        # The value should remain as-is since decryption silently fails
        assert result["password"] == "plaintext-value"

    def test_encrypt_dict_skips_empty_fields(self):
        key = generate_key()
        data = {"host": "localhost", "password": "", "token": None}
        encrypted = encrypt_dict(data, key, ["password", "token"])
        # Empty and None values should be left untouched
        assert encrypted["password"] == ""
        assert encrypted["token"] is None


class TestGenerateKey:
    """Key generation."""

    def test_generate_key(self):
        key = generate_key()
        assert isinstance(key, str)
        assert len(key) > 0
        # Should be a valid Fernet key (base64-encoded, 44 chars)
        assert len(key) == 44

    def test_generate_key_unique(self):
        keys = {generate_key() for _ in range(10)}
        assert len(keys) == 10, "Generated keys should be unique"
