import logging
import sys
from typing import Any, Dict, Optional
from queue import Queue
from logging.handlers import QueueHandler, QueueListener
import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import json
import threading
from .logger_config import ElasticLoggerConfig
import traceback

class ElasticsearchHandler(logging.Handler):
    def __init__(self, es_config: Dict[str, Any], index: str):
        super().__init__()
        self.es_config = es_config
        self.index = index
        self.buffer = Queue()
        self.es = None
        self.flush_interval = 10  # seconds
        self.stopping = False
        self.loop = asyncio.new_event_loop()
        self.flush_thread = threading.Thread(target=self._run_event_loop)
        self.flush_thread.daemon = True
        self.flush_thread.start()

    
    def emit(self, record):
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            formatted_traceback = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
        else:
            formatted_traceback = None

        log_entry = {
            'timestamp': record.created,
            'level': record.levelname,
            'message': record.getMessage(),
            'service_name': record.name,
            'traceback': formatted_traceback
        }
        self.buffer.put(log_entry)
    def _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._flush_loop())

    async def _flush_loop(self):
        while not self.stopping:
            await asyncio.sleep(self.flush_interval)
            await self._flush()

    async def _flush(self):
        if self.buffer.empty():
            return

        if not self.es:
            self.es = AsyncElasticsearch(**self.es_config)

        logs = []
        while not self.buffer.empty():
            try:
                logs.append(self.buffer.get_nowait())
            except:
                break

        if logs:
            try:
                actions = [
                    {
                        '_index': self.index,
                        '_source': json.dumps(log_entry)
                    }
                    for log_entry in logs
                ]
                await async_bulk(self.es, actions)
            except Exception as e:
                print(f"Error sending logs to Elasticsearch: {e}", file=sys.stderr)

    def close(self):
        self.stopping = True
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.flush_thread.join()
        if self.es:
            self.loop.run_until_complete(self.es.close())
        self.loop.close()
        super().close()

class AsyncLogger:
    def __init__(self, name: str = "FastAPI", level: int = logging.INFO, 
                 es_config: Optional[Dict[str, Any]] = None, 
                 es_index: str = "logs",
                 es_level: int = logging.WARNING):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        self.log_queue = Queue()
        queue_handler = QueueHandler(self.log_queue)
        self.logger.addHandler(queue_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        handlers: list[logging.Handler] = [console_handler]

        if es_config:
            es_handler = ElasticsearchHandler(es_config, es_index)
            es_handler.setLevel(es_level)
            handlers.append(es_handler)

        self.queue_listener = QueueListener(self.log_queue, *handlers, respect_handler_level=True)
        self.queue_listener.start()

    def __del__(self):
        self.queue_listener.stop()
        for handler in self.queue_listener.handlers:
            if isinstance(handler, ElasticsearchHandler):
                handler.close()

    async def log(self, level: int, msg: Any) -> None:
        self.logger.log(level, msg)

    async def info(self, msg: Any) -> None:
        await self.log(logging.INFO, msg)

    async def error(self, msg: Any) -> None:
        await self.log(logging.ERROR, msg)

    async def warning(self, msg: Any) -> None:
        await self.log(logging.WARNING, msg)

    async def debug(self, msg: Any) -> None:
        await self.log(logging.DEBUG, msg)


def get_log_level(level: str):
    level = level.upper()
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }.get(level, logging.WARNING)
    

_logger_instance = None
_logger_lock = threading.Lock()

def get_logger(elastic_logger_configs: ElasticLoggerConfig):
    global _logger_instance
    with _logger_lock: 
        if _logger_instance is None:
            es_config = {
                'hosts': [elastic_logger_configs.elastic_url],
                'http_auth': (elastic_logger_configs.elastic_username, elastic_logger_configs.elastic_password),
            }
            _logger_instance = AsyncLogger(
                es_config=es_config, 
                es_level=get_log_level(elastic_logger_configs.elastic_log_level), 
                es_index=elastic_logger_configs.elastic_log_index_name
            )
    return _logger_instance