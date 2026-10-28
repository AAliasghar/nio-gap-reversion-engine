import pandas as pd

from scripts.config import get_database_url, get_db_properties, get_jdbc_url


def test_database_url_supports_env_overrides(monkeypatch):
    monkeypatch.setenv("NIO_DB_HOST", "example-host")
    monkeypatch.setenv("NIO_DB_PORT", "6543")
    monkeypatch.setenv("NIO_DB_NAME", "example_db")
    monkeypatch.setenv("NIO_DB_USER", "example_user")
    monkeypatch.setenv("NIO_DB_PASSWORD", "example_password")

    url = get_database_url()

    assert "example-host" in url
    assert "6543" in url
    assert "example_db" in url
    assert "example_user" in url
    assert "example_password" in url


def test_jdbc_url_and_properties_use_env(monkeypatch):
    monkeypatch.setenv("NIO_DB_HOST", "example-host")
    monkeypatch.setenv("NIO_DB_PORT", "6543")
    monkeypatch.setenv("NIO_DB_NAME", "example_db")
    monkeypatch.setenv("NIO_DB_USER", "example_user")
    monkeypatch.setenv("NIO_DB_PASSWORD", "example_password")
    monkeypatch.delenv("NIO_JDBC_URL", raising=False)

    assert get_jdbc_url() == "jdbc:postgresql://example-host:6543/example_db"
    assert get_db_properties()["user"] == "example_user"
    assert get_db_properties()["password"] == "example_password"


def test_ema_calculation_is_not_a_flat_last_value():
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    ema = series.ewm(span=3, adjust=False).mean()

    assert ema.iloc[0] == 1.0
    assert ema.iloc[1] > 1.0
    assert ema.iloc[-1] > series.iloc[-2]
