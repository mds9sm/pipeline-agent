"""Tests for sandbox.py -- connector code validation and sandboxed execution."""

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sandbox import validate_connector_code, safe_exec


# ======================================================================
# validate_connector_code tests
# ======================================================================


class TestValidateConnectorCode:

    def test_valid_connector_code(self):
        """Simple class with allowed imports passes validation."""
        code = """
import json
import csv
from datetime import datetime

class MyConnector:
    def __init__(self):
        self.name = "test"

    def extract(self):
        return json.dumps({"rows": 100})
"""
        valid, err = validate_connector_code(code)
        assert valid is True
        assert err == ""

    def test_blocked_import_subprocess(self):
        """Code importing subprocess fails validation."""
        code = "import subprocess\nsubprocess.run(['ls'])"
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "subprocess" in err

    def test_blocked_import_os(self):
        """Code importing os directly fails validation."""
        code = "import os\nos.listdir('/')"
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "os" in err.lower()

    def test_allowed_import_os_path(self):
        """Code importing os.path passes validation (explicitly allowed)."""
        code = """
import os.path

def get_ext(filename):
    return os.path.splitext(filename)[1]
"""
        valid, err = validate_connector_code(code)
        assert valid is True
        assert err == ""

    def test_blocked_eval_call(self):
        """Code calling eval() fails validation."""
        code = """
x = eval("1 + 2")
"""
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "eval" in err

    def test_blocked_os_system(self):
        """Code calling os.system() fails validation."""
        code = """
import os.path
os.system("echo hello")
"""
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "os.system" in err

    def test_syntax_error(self):
        """Invalid Python fails validation with syntax error message."""
        code = "def broken(:\n    pass"
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "Syntax error" in err or "syntax" in err.lower()

    def test_blocked_from_import(self):
        """from subprocess import ... is blocked."""
        code = "from subprocess import Popen"
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "subprocess" in err

    def test_blocked_exec_call(self):
        """Code calling exec() fails validation."""
        code = 'exec("print(1)")'
        valid, err = validate_connector_code(code)
        assert valid is False
        assert "exec" in err


# ======================================================================
# safe_exec tests
# ======================================================================


class TestSafeExec:

    def test_safe_exec_loads_class(self):
        """safe_exec with valid code returns namespace with class."""
        code = """
import json

class DataProcessor:
    def process(self, data):
        return json.dumps(data)
"""
        ns = safe_exec(code)
        assert "DataProcessor" in ns
        # The class should be usable
        instance = ns["DataProcessor"]()
        assert instance.process({"a": 1}) == '{"a": 1}'

    def test_safe_exec_blocks_import(self):
        """safe_exec raises ImportError for blocked module."""
        code = "import subprocess"
        with pytest.raises(ImportError, match="not allowed"):
            safe_exec(code)

    def test_safe_exec_blocks_socket(self):
        """safe_exec blocks socket import."""
        code = "import socket"
        with pytest.raises(ImportError):
            safe_exec(code)

    def test_safe_exec_allows_datetime(self):
        """safe_exec allows datetime import."""
        code = """
from datetime import datetime
now = datetime.utcnow()
"""
        ns = safe_exec(code)
        assert "now" in ns

    def test_safe_exec_extra_globals(self):
        """Extra globals are available inside safe_exec."""
        code = "result = injected_value * 2"
        ns = safe_exec(code, extra_globals={"injected_value": 21})
        assert ns["result"] == 42
