from core.kafka.schemas import IndicatorEvent
from src.services.selector_service import SelectorService, Route
from infra.kafka.topics import TOPIC_INDICATORS_REALTIME, TOPIC_INDICATORS_PERSISTED


def _make_indicator(name: str = "SMA") -> IndicatorEvent:
    return IndicatorEvent(
        symbol="btcusdt",
        interval="1s",
        indicator_name=name,
        value=50000.0,
        timestamp=1000,
    )


class TestSelectorService:
    def test_default_route_both(self):
        service = SelectorService()
        topics = service.route(_make_indicator())

        assert TOPIC_INDICATORS_REALTIME in topics
        assert TOPIC_INDICATORS_PERSISTED in topics
        assert len(topics) == 2

    def test_route_realtime_only(self):
        service = SelectorService(default_route=Route.REALTIME)
        topics = service.route(_make_indicator())

        assert topics == [TOPIC_INDICATORS_REALTIME]

    def test_route_persisted_only(self):
        service = SelectorService(default_route=Route.PERSISTED)
        topics = service.route(_make_indicator())

        assert topics == [TOPIC_INDICATORS_PERSISTED]

    def test_per_indicator_override(self):
        service = SelectorService(default_route=Route.BOTH)
        service.set_route("RSI", Route.REALTIME)

        rsi_topics = service.route(_make_indicator("RSI"))
        sma_topics = service.route(_make_indicator("SMA"))

        assert rsi_topics == [TOPIC_INDICATORS_REALTIME]
        assert len(sma_topics) == 2

    def test_multiple_overrides(self):
        service = SelectorService(default_route=Route.BOTH)
        service.set_route("RSI", Route.REALTIME)
        service.set_route("VWAP", Route.PERSISTED)

        assert service.route(_make_indicator("RSI")) == [TOPIC_INDICATORS_REALTIME]
        assert service.route(_make_indicator("VWAP")) == [TOPIC_INDICATORS_PERSISTED]
        assert len(service.route(_make_indicator("SMA"))) == 2
