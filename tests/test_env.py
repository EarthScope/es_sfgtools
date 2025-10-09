import os

import pytest

import env


@pytest.fixture
def set_env_var():
    name = "FOO"
    value = "42"
    os.environ[name] = value
    yield name, value
    del os.environ[name]


class TestGetEnv:
    def test_get_env(self, set_env_var):
        name, value = set_env_var
        assert env.get_env(name) == value, "Returns env var value"

    def test_get_env_missing(self):
        assert env.get_env("FOO") is None, "Returns None for missing env var"

    def test_get_env_default(self):
        assert (
            env.get_env("FOO", "BAR") == "BAR"
        ), "Returns default arg for missing env var"


class TestGetEnvRequired:
    def test_get_env_required(self, set_env_var):
        name, value = set_env_var
        assert env.get_env_required(name) == value, "Returns env var value"

    def test_get_env_required_missing(self):
        with pytest.raises(KeyError):
            env.get_env_required("FOO")

class TestGarposInstall:
    def is_garpos_installed(self):
        import importlib.util
        garpos_spec = importlib.util.find_spec("garpos")
        return garpos_spec is not None
    def test_garpos_install(self):
        assert self.is_garpos_installed(), "Garpos not installed"