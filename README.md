
# Async Logger

Async Logger is a custom Python logging library designed to log messages asynchronously to both the console and Elasticsearch. 
It is built on top of Python's `logging` module and integrates with Elasticsearch using the `AsyncElasticsearch` client. 
This allows for non-blocking, asynchronous logging in distributed environments.

## Features

- Asynchronous logging to Elasticsearch.
- Buffered log messages with periodic flushing to avoid performance hits.
- Customizable logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL).
- Integrated console logging.

## Installation

Install the package using pip:

```bash
pip install async_elastic_logger
```

## Usage

Here’s an example of how to use `AsyncLogger` in your Python application.

### 1. Define your configuration class using `pydantic`

You'll need to use `pydantic` to define a settings class that contains your Elasticsearch configurations.

```python
from pydantic import BaseSettings

class ElasticLoggerConfig(BaseSettings):
    elastic_url: str
    elastic_username: str
    elastic_password: str
    elastic_log_level: str = "WARNING"
    elastic_log_index_name: str = "logs"
```

### 2. Use the `get_logger` function to get the singleton logger

Once you have the configuration, you can get the logger instance using the `get_logger` function. The logger is created as a singleton, so the same instance will be returned every time you call `get_logger`.

```python
from async_logger.logger import get_logger, ElasticLoggerConfig

# Define your Elasticsearch logger configurations
config = ElasticLoggerConfig(
    elastic_url="https://your-elasticsearch-url",
    elastic_username="your-username",
    elastic_password="your-password",
    elastic_log_level="INFO",  # or DEBUG, ERROR, etc.
    elastic_log_index_name="your-log-index"
)

# Get the singleton logger instance
logger = get_logger(config)

# Log messages
await logger.info("This is an info message")
await logger.error("This is an error message")
await logger.debug("This is a debug message")
```

### 3. Handling log levels

You can specify the logging level for Elasticsearch logs using the `elastic_log_level` field in your configuration. It accepts values like `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

For example:

```python
config = ElasticLoggerConfig(
    elastic_url="https://your-elasticsearch-url",
    elastic_username="your-username",
    elastic_password="your-password",
    elastic_log_level="DEBUG",  # Logs at DEBUG level
    elastic_log_index_name="your-log-index"
)
```

### 4. Configuration flexibility

You can easily switch the log level or Elasticsearch index by updating the configuration parameters. This allows you to fine-tune logging based on the environment or the level of detail you want in your logs.

### 5. Singleton behavior

Once the logger is instantiated with the configuration, subsequent calls to `get_logger` will return the same instance, ensuring that logging across your application is handled consistently.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
