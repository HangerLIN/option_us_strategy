from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping, Protocol, Sequence

import pandas as pd

from libs.portfolio import PortfolioDecision
from libs.schemas.assets import AssetType, BarEvent, InstrumentRef, MarketQuote
from libs.schemas.exec import ExecutionRequest
from libs.schemas.risk import RiskCheckRequest, RiskCheckResult
from libs.schemas.signals import SignalEnvelope


@dataclass(frozen=True, slots=True)
class StrategyPluginMetadata:
    strategy_code: str
    supported_asset_types: set[AssetType]
    supported_tracks: set[str] = field(default_factory=lambda: {"equity", "option"})
    requires_option_chain: bool = False
    requires_equity_indicators: bool = True
    version: str = "v1"


class DataIngestionAdapter(Protocol):
    def ingest(
        self,
        *,
        instruments: Sequence[InstrumentRef],
        start: datetime,
        end: datetime,
        context: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        ...


class FeatureBuilder(Protocol):
    def build(
        self,
        *,
        bars: pd.DataFrame,
        quotes: Sequence[MarketQuote] | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        ...


class SignalPlugin(Protocol):
    metadata: StrategyPluginMetadata

    def process_bar(
        self,
        event: BarEvent,
        *,
        features: Mapping[str, Any],
        context: Mapping[str, Any] | None = None,
    ) -> list[SignalEnvelope]:
        ...


class RiskRulePlugin(Protocol):
    def evaluate(
        self,
        request: RiskCheckRequest,
        *,
        exposures: Sequence[Any],
        context: Mapping[str, Any] | None = None,
    ) -> RiskCheckResult:
        ...


class ExecutionSelectionPlugin(Protocol):
    def build_request(
        self,
        decision: PortfolioDecision,
        *,
        quote: MarketQuote | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> ExecutionRequest:
        ...


class BacktestStrategyPlugin(Protocol):
    metadata: StrategyPluginMetadata

    def replay(
        self,
        *,
        start: datetime,
        end: datetime,
        symbols: Sequence[str],
        track: str,
        context: Mapping[str, Any] | None = None,
    ) -> Iterable[SignalEnvelope]:
        ...


class PerformanceReporter(Protocol):
    def build_report(
        self,
        *,
        run_ids: Sequence[int],
        context: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        ...
