import os


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default or ""
    return value


def get_database_url() -> str:
    return _env(
        "NIO_DB_URL",
        "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=_env("NIO_DB_USER", "quant_user"),
            password=_env("NIO_DB_PASSWORD", "quant_password"),
            host=_env("NIO_DB_HOST", "localhost"),
            port=_env("NIO_DB_PORT", "5432"),
            database=_env("NIO_DB_NAME", "trading_warehouse"),
        ),
    )


def get_jdbc_url() -> str:
    return _env(
        "NIO_JDBC_URL",
        "jdbc:postgresql://{host}:{port}/{database}".format(
            host=_env("NIO_DB_HOST", "nio_postgres"),
            port=_env("NIO_DB_PORT", "5432"),
            database=_env("NIO_DB_NAME", "trading_warehouse"),
        ),
    )


def get_db_properties() -> dict:
    return {
        "user": _env("NIO_DB_USER", "quant_user"),
        "password": _env("NIO_DB_PASSWORD", "quant_password"),
        "driver": "org.postgresql.Driver",
    }
