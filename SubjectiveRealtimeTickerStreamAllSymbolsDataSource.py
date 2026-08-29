import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from subjective_abstract_data_source_package import SubjectiveDataSource

from trading_contracts.plugin_support import (
    TICKER_OUTPUT_SCHEMA,
    empty_ticker,
    icon_for,
    ticker_stream,
)


class SubjectiveRealtimeTickerStreamAllSymbolsDataSource(SubjectiveDataSource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.quote_filter = str(self._connection.get("quote_filter", "USDT")).upper()

    @classmethod
    def connection_schema(cls):
        return {"quote_filter": {"type": "text", "label": "Quote Filter", "default": "USDT"}}

    @classmethod
    def request_schema(cls):
        return {"quote_filter": {"type": "text", "label": "Quote Filter"}, "events": {"type": "array", "label": "Injected Events"}}

    @classmethod
    def output_schema(cls):
        return TICKER_OUTPUT_SCHEMA

    @classmethod
    def icon(cls):
        return icon_for(__file__)

    def supports_streaming(self):
        return True

    def stream(self, request):
        yield from ticker_stream(request or {}, {**self._connection, "quote_filter": self.quote_filter}, "all")

    def run(self, request):
        try:
            event = next(self.stream(request or {}))
            if event.get("error"):
                return empty_ticker(event["error"])
            return {**event, "error": ""}
        except StopIteration:
            return empty_ticker()
