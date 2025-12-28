import time
from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveRealtimeTickerStreamAllSymbolsDataSource(SubjectiveDataSource):
    connection_type = "Socket"
    connection_fields = ["host", "port", "local_source"]
    icon_svg = "<svg width='24' height='24' viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'><circle cx='12' cy='12' r='9' fill='#2d6a4f'/><path d='M7 12h10' stroke='#ffffff' stroke-width='2'/></svg>"

    def get_icon(self):
        return self.icon_svg

    def get_connection_data(self):
        return {"connection_type": self.connection_type, "fields": list(self.connection_fields)}

    def _get_param(self, key, default=None):
        return self.params.get(key, default)

    def _on_tick(self, data):
        self.update(data)
        self.increment_processed_items()

    def start(self):
        from com_goldenthinker_trade_exchange.ExchangeConfiguration import ExchangeConfiguration

        self.set_total_items(0)
        self.set_processed_items(0)
        exchange = ExchangeConfiguration.get_default_exchange()
        local_source = bool(self._get_param("local_source", False))
        if local_source:
            exchange.subscribe_to_local_datasource_ticker(callback=self._on_tick)
        else:
            exchange.subscribe_all_symbols_stream(callback=self._on_tick)

    def fetch(self):
        if self.status_callback:
            self.status_callback(self.get_name(), "stream_started")
        self.start()
        self.set_fetch_completed(True)
        BBLogger.log(f"Stream started for {self.get_name()}")
