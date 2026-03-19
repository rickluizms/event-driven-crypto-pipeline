from enum import Enum
from core.kafka.schemas import IndicatorEvent
from infra.kafka.topics import TOPIC_INDICATORS_REALTIME, TOPIC_INDICATORS_PERSISTED


class Route(Enum):
    REALTIME = "realtime"
    PERSISTED = "persisted"
    BOTH = "both"


ROUTE_TO_TOPICS = {
    Route.REALTIME: [TOPIC_INDICATORS_REALTIME],
    Route.PERSISTED: [TOPIC_INDICATORS_PERSISTED],
    Route.BOTH: [TOPIC_INDICATORS_REALTIME, TOPIC_INDICATORS_PERSISTED],
}


class SelectorService:
    def __init__(self, default_route: Route = Route.BOTH):
        self._default_route = default_route
        self._routes: dict[str, Route] = {}

    def set_route(self, indicator_name: str, route: Route) -> None:
        self._routes[indicator_name] = route

    def route(self, indicator: IndicatorEvent) -> list[str]:
        indicator_route = self._routes.get(indicator.indicator_name, self._default_route)
        return ROUTE_TO_TOPICS[indicator_route]
