from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Literal

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralised application configuration sourced from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: Literal["development", "staging", "production", "test"] = "development"
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    database_url: str = Field(..., validation_alias="DATABASE_URL")
    redis_url: str = Field(..., validation_alias="REDIS_URL")

    prometheus_host: str = Field("0.0.0.0", validation_alias="PROMETHEUS_HOST")
    prometheus_port: int = Field(9000, validation_alias="PROMETHEUS_PORT")

    ib_host: str = Field(..., validation_alias="IB_HOST")
    ib_port: int = Field(..., validation_alias="IB_PORT")
    ib_client_id: int = Field(..., validation_alias="IB_CLIENT_ID")
    ib_account: str = Field(..., validation_alias="IB_ACCOUNT")
    paper_trading: bool = Field(True, validation_alias="PAPER")
    vix_required: bool = Field(True, validation_alias="VIX_REQUIRED")
    ib_market_bucket: int = Field(20, validation_alias="IB_MKT_BUCKET")
    ib_market_refill_seconds: int = Field(1, validation_alias="IB_MKT_REFILL_SECONDS")
    ib_historical_bucket: int = Field(4, validation_alias="IB_HIST_BUCKET")
    ib_historical_refill_seconds: int = Field(60, validation_alias="IB_HIST_REFILL_SECONDS")
    ib_scanner_bucket: int = Field(2, validation_alias="IB_SCANNER_BUCKET")
    ib_scanner_refill_seconds: int = Field(30, validation_alias="IB_SCANNER_REFILL_SECONDS")
    ib_contract_bucket: int = Field(8, validation_alias="IB_CONTRACT_BUCKET")
    ib_contract_refill_seconds: int = Field(60, validation_alias="IB_CONTRACT_REFILL_SECONDS")
    ib_selfcheck_symbol: str = Field("SPY", validation_alias="IB_SELFCHECK_SYMBOL")

    timezone: str = Field("US/Eastern", validation_alias="TZ")
    risk_notional_cap: Decimal = Field(Decimal("5000000"), validation_alias="RISK_NOTIONAL_CAP")
    vix_gate: Decimal = Field(Decimal("20"), validation_alias="VIX_GATE")
    vix_gate_mode: Literal["enforce", "record", "ignore"] = Field(
        "enforce", validation_alias="VIX_GATE_MODE"
    )
    ttl_buy_seconds: int = Field(180, validation_alias="TTL_BUY")
    cooldown_buy_seconds: int = Field(600, validation_alias="COOLDOWN_BUY")
    feature_mfi_stoch_filter: bool = Field(False, validation_alias="FEATURE_MFI_STOCH_FILTER")
    daily_loss_r: Decimal = Field(Decimal("3"), validation_alias="DAILY_LOSS_R")
    max_concurrent_top: int = Field(3, validation_alias="MAX_CONCURRENT_TOP")
    rvol_baseline_days: int = Field(20, validation_alias="RVOL_BASELINE_DAYS")
    symbol_block_minutes: int = Field(10, validation_alias="SYMBOL_BLOCK_MINUTES")
    risk_unit_value: Decimal = Field(Decimal("1000"), validation_alias="RISK_UNIT_VALUE")
    iv_overnight_call_cap: Decimal = Field(
        Decimal("1.0"), validation_alias="IV_OVERNIGHT_CALL_CAP"
    )
    preearn_days_min: int = Field(3, validation_alias="PRE_EARN_DAYS_MIN")
    preearn_days_max: int = Field(5, validation_alias="PRE_EARN_DAYS_MAX")
    preearn_atr_pct_max: Decimal = Field(
        Decimal("0.02"), validation_alias="PRE_EARN_ATR_PCT_MAX"
    )
    risk_service_url: str = Field("http://localhost:8082", validation_alias="RISK_SERVICE_URL")
    option_bar_table: str = Field("bars1m_option", validation_alias="OPTION_BAR_TABLE")
    option_chain_table: str = Field("option_chain_meta", validation_alias="OPTION_CHAIN_TABLE")
    option_liquidity_required: bool = Field(
        False, validation_alias="OPTION_LIQUIDITY_REQUIRED"
    )
    option_strike_lower_pct: Decimal = Field(Decimal("0.5"), validation_alias="OPTION_STRIKE_LOWER_PCT")
    option_strike_upper_pct: Decimal = Field(Decimal("1.5"), validation_alias="OPTION_STRIKE_UPPER_PCT")
    hist_backfill_window_min: int = Field(15, validation_alias="HIST_BACKFILL_WINDOW_MIN")
    backtest_strategy_variant: str = Field("v1", validation_alias="BACKTEST_STRATEGY_VARIANT")
    backtest_orphan_timeout_hours: int = Field(
        24, validation_alias="BACKTEST_ORPHAN_TIMEOUT_HOURS"
    )
    calibration_version: str | None = Field(None, validation_alias="CALIBRATION_VERSION")
    open_chase_earliest_entry_time: str = Field(
        "09:36", validation_alias="OPEN_CHASE_EARLIEST_ENTRY_TIME"
    )
    open_chase_opening_range_minutes: int = Field(
        5, validation_alias="OPEN_CHASE_OPENING_RANGE_MINUTES"
    )
    open_chase_max_signal_to_fill_seconds: int = Field(
        90, validation_alias="OPEN_CHASE_MAX_SIGNAL_TO_FILL_SECONDS"
    )
    open_chase_max_underlying_adverse_move_pct: Decimal = Field(
        Decimal("0.0025"), validation_alias="OPEN_CHASE_MAX_UNDERLYING_ADVERSE_MOVE_PCT"
    )
    open_chase_require_above_vwap_at_fill: bool = Field(
        True, validation_alias="OPEN_CHASE_REQUIRE_ABOVE_VWAP_AT_FILL"
    )
    open_chase_require_above_orh_at_fill: bool = Field(
        False, validation_alias="OPEN_CHASE_REQUIRE_ABOVE_ORH_AT_FILL"
    )
    open_chase_reject_option_spread_pct_gt: Decimal = Field(
        Decimal("0.10"), validation_alias="OPEN_CHASE_REJECT_OPTION_SPREAD_PCT_GT"
    )
    open_chase_max_upper_shadow_pct: Decimal = Field(
        Decimal("0.40"), validation_alias="OPEN_CHASE_MAX_UPPER_SHADOW_PCT"
    )
    open_chase_first_bar_blowoff_range_pct: Decimal = Field(
        Decimal("0.012"), validation_alias="OPEN_CHASE_FIRST_BAR_BLOWOFF_RANGE_PCT"
    )
    open_chase_min_rvol: Decimal = Field(
        Decimal("1.0"), validation_alias="OPEN_CHASE_MIN_RVOL"
    )
    open_chase_require_market_confirmed: bool = Field(
        False, validation_alias="OPEN_CHASE_REQUIRE_MARKET_CONFIRMED"
    )
    open_chase_max_option_loss_pct: Decimal = Field(
        Decimal("0.20"), validation_alias="OPEN_CHASE_MAX_OPTION_LOSS_PCT"
    )
    open_chase_no_new_high_minutes: int = Field(
        8, validation_alias="OPEN_CHASE_NO_NEW_HIGH_MINUTES"
    )
    open_chase_hard_stop_below_entry_bar_low: bool = Field(
        True, validation_alias="OPEN_CHASE_HARD_STOP_BELOW_ENTRY_BAR_LOW"
    )
    open_chase_hard_stop_below_vwap: bool = Field(
        True, validation_alias="OPEN_CHASE_HARD_STOP_BELOW_VWAP"
    )
    open_chase_first_take_profit_pct: Decimal = Field(
        Decimal("0.25"), validation_alias="OPEN_CHASE_FIRST_TAKE_PROFIT_PCT"
    )
    open_chase_second_take_profit_pct: Decimal = Field(
        Decimal("0.50"), validation_alias="OPEN_CHASE_SECOND_TAKE_PROFIT_PCT"
    )
    open_chase_min_hold_seconds_before_upper_tap_exit: int = Field(
        300, validation_alias="OPEN_CHASE_MIN_HOLD_SECONDS_BEFORE_UPPER_TAP_EXIT"
    )
    open_chase_require_profit_for_upper_tap_exit: bool = Field(
        True, validation_alias="OPEN_CHASE_REQUIRE_PROFIT_FOR_UPPER_TAP_EXIT"
    )
    open_chase_profit_must_cover_spread_and_fee: bool = Field(
        True, validation_alias="OPEN_CHASE_PROFIT_MUST_COVER_SPREAD_AND_FEE"
    )
    open_chase_no_profit_exit_minutes: int = Field(
        10, validation_alias="OPEN_CHASE_NO_PROFIT_EXIT_MINUTES"
    )
    open_chase_trend_confirm_deadline: str = Field(
        "10:00", validation_alias="OPEN_CHASE_TREND_CONFIRM_DEADLINE"
    )
    open_chase_eod_exit_time: str = Field(
        "15:55", validation_alias="OPEN_CHASE_EOD_EXIT_TIME"
    )
    reversal_1030_symbols: str = Field(
        "TSLA,PLTR,TQQQ,GOOGL,AAPL", validation_alias="REVERSAL_1030_SYMBOLS"
    )
    reversal_1030_start_time: str = Field(
        "10:25", validation_alias="REVERSAL_1030_START_TIME"
    )
    reversal_1030_end_time: str = Field(
        "10:40", validation_alias="REVERSAL_1030_END_TIME"
    )
    reversal_1030_exit_deadline: str = Field(
        "12:00", validation_alias="REVERSAL_1030_EXIT_DEADLINE"
    )
    reversal_1030_max_vix: Decimal = Field(
        Decimal("20"), validation_alias="REVERSAL_1030_MAX_VIX"
    )
    reversal_1030_max_abs_gap_pct: Decimal = Field(
        Decimal("0.06"), validation_alias="REVERSAL_1030_MAX_ABS_GAP_PCT"
    )
    reversal_1030_min_morning_drop_pct: Decimal = Field(
        Decimal("0.003"), validation_alias="REVERSAL_1030_MIN_MORNING_DROP_PCT"
    )
    reversal_1030_lower_band_buffer_pct: Decimal = Field(
        Decimal("0.0015"), validation_alias="REVERSAL_1030_LOWER_BAND_BUFFER_PCT"
    )
    reversal_1030_rsi_reclaim: Decimal = Field(
        Decimal("30"), validation_alias="REVERSAL_1030_RSI_RECLAIM"
    )
    reversal_1030_max_boll_pos_low: Decimal = Field(
        Decimal("0.55"), validation_alias="REVERSAL_1030_MAX_BOLL_POS_LOW"
    )
    reversal_1030_max_boll_pos_reclaim: Decimal = Field(
        Decimal("0.60"), validation_alias="REVERSAL_1030_MAX_BOLL_POS_RECLAIM"
    )
    reversal_1030_max_rsi6: Decimal = Field(
        Decimal("65"), validation_alias="REVERSAL_1030_MAX_RSI6"
    )
    reversal_1030_reclaim_max_distance_vwap_pct: Decimal = Field(
        Decimal("0.0025"), validation_alias="REVERSAL_1030_RECLAIM_MAX_DISTANCE_VWAP_PCT"
    )
    reversal_1030_mid_reclaim_max_distance_pct: Decimal = Field(
        Decimal("0.0010"), validation_alias="REVERSAL_1030_MID_RECLAIM_MAX_DISTANCE_PCT"
    )
    reversal_1030_mid_reclaim_min_upper_room_pct: Decimal = Field(
        Decimal("0.0035"), validation_alias="REVERSAL_1030_MID_RECLAIM_MIN_UPPER_ROOM_PCT"
    )
    reversal_1030_mid_reclaim_loose_min_upper_room_pct: Decimal = Field(
        Decimal("0.0030"), validation_alias="REVERSAL_1030_MID_RECLAIM_LOOSE_MIN_UPPER_ROOM_PCT"
    )
    reversal_1030_mid_reclaim_room040_min_upper_room_pct: Decimal = Field(
        Decimal("0.0040"), validation_alias="REVERSAL_1030_MID_RECLAIM_ROOM040_MIN_UPPER_ROOM_PCT"
    )
    reversal_1030_mid_reclaim_strict_max_boll_pos: Decimal = Field(
        Decimal("0.55"), validation_alias="REVERSAL_1030_MID_RECLAIM_STRICT_MAX_BOLL_POS"
    )
    reversal_1030_mid_reclaim_strict_min_upper_room_pct: Decimal = Field(
        Decimal("0.0035"), validation_alias="REVERSAL_1030_MID_RECLAIM_STRICT_MIN_UPPER_ROOM_PCT"
    )
    reversal_1030_max_upper_shadow_pct: Decimal = Field(
        Decimal("0.40"), validation_alias="REVERSAL_1030_MAX_UPPER_SHADOW_PCT"
    )
    reversal_1030_failure_stop_entry_low_buffer_pct: Decimal = Field(
        Decimal("0.0005"), validation_alias="REVERSAL_1030_FAILURE_STOP_ENTRY_LOW_BUFFER_PCT"
    )
    reversal_1030_reclaim_lost_buffer_pct: Decimal = Field(
        Decimal("0.0005"), validation_alias="REVERSAL_1030_RECLAIM_LOST_BUFFER_PCT"
    )
    reversal_1030_v8_guard_boll_pos_threshold: Decimal = Field(
        Decimal("0.30"), validation_alias="REVERSAL_1030_V8_GUARD_BOLL_POS_THRESHOLD"
    )
    reversal_1030_v8_guard_rsi_floor_boll_pos_threshold: Decimal = Field(
        Decimal("0.20"),
        validation_alias="REVERSAL_1030_V8_GUARD_RSI_FLOOR_BOLL_POS_THRESHOLD",
    )
    reversal_1030_v8_guard_rsi_floor_min_rsi6: Decimal = Field(
        Decimal("35"), validation_alias="REVERSAL_1030_V8_GUARD_RSI_FLOOR_MIN_RSI6"
    )
    reversal_1030_v8_guard_core_room_025_min_upper_room_pct: Decimal = Field(
        Decimal("0.0025"),
        validation_alias="REVERSAL_1030_V8_GUARD_CORE_ROOM_025_MIN_UPPER_ROOM_PCT",
    )
    reversal_1030_v8_guard_core_room_035_min_upper_room_pct: Decimal = Field(
        Decimal("0.0035"),
        validation_alias="REVERSAL_1030_V8_GUARD_CORE_ROOM_035_MIN_UPPER_ROOM_PCT",
    )
    reversal_1030_v8_guard_core_max_boll_pos: Decimal = Field(
        Decimal("0.80"), validation_alias="REVERSAL_1030_V8_GUARD_CORE_MAX_BOLL_POS"
    )
    reversal_1030_v8_guard_continuation_min_boll_pos: Decimal = Field(
        Decimal("0.60"), validation_alias="REVERSAL_1030_V8_GUARD_CONTINUATION_MIN_BOLL_POS"
    )
    reversal_1030_early_followthrough_check_minutes: int = Field(
        3, validation_alias="REVERSAL_1030_EARLY_FOLLOWTHROUGH_CHECK_MINUTES"
    )
    reversal_1030_early_followthrough_min_mfe_pct: Decimal = Field(
        Decimal("0.0010"), validation_alias="REVERSAL_1030_EARLY_FOLLOWTHROUGH_MIN_MFE_PCT"
    )
    reversal_1030_boll_up_true_profit_min_pct: Decimal = Field(
        Decimal("0"), validation_alias="REVERSAL_1030_BOLL_UP_TRUE_PROFIT_MIN_PCT"
    )
    reversal_1030_continuation_min_exit_profit_pct: Decimal = Field(
        Decimal("0.0030"), validation_alias="REVERSAL_1030_CONTINUATION_MIN_EXIT_PROFIT_PCT"
    )
    reversal_1030_continuation_max_vwap_distance_pct: Decimal = Field(
        Decimal("0.0075"), validation_alias="REVERSAL_1030_CONTINUATION_MAX_VWAP_DISTANCE_PCT"
    )
    reversal_1030_continuation_fail_buffer_pct: Decimal = Field(
        Decimal("0.0003"), validation_alias="REVERSAL_1030_CONTINUATION_FAIL_BUFFER_PCT"
    )
    reversal_1030_thin_room_filter_pct: Decimal = Field(
        Decimal("0.0010"), validation_alias="REVERSAL_1030_THIN_ROOM_FILTER_PCT"
    )
    reversal_1030_thin_room_filter_min_boll_pos: Decimal = Field(
        Decimal("0.80"), validation_alias="REVERSAL_1030_THIN_ROOM_FILTER_MIN_BOLL_POS"
    )
    
    # Monitoring
    monitoring_enabled: bool = Field(False, validation_alias="MONITORING_ENABLED")
    monitoring_port: int = Field(9106, validation_alias="MONITORING_PORT")
    monitoring_env: str = Field("paper", validation_alias="MONITORING_ENV")
    
    # Top5 股票池配置
    top5_mode: Literal["scanner", "fixed_pool"] = Field("fixed_pool", validation_alias="TOP5_MODE")
    top5_fixed_pool: str = Field(
        "NVDA,MSFT,AAPL,GOOGL,AMZN,META,AVGO,TSM,TSLA,ORCL,"
        "NFLX,PLTR,ASML,CSCO,AMD,CRM,DIS,UBER,SHOP,NOW,"
        "INTU,ANET,QCOM,TXN,APP,ADBE,ARM,GOOG,PANW,LRCX,"
        "AMAT,ADI,KLAC,INTC,CRWD,CDNS,SNPS,DELL,COIN,EQIX,"
        "SNOW,NET,WDAY,FTNT,NXPI,MRVL,DDOG,VEEV,TEAM,ZS,"
        "MCHP,HPE,AFRM,MDB,ZM,HUBS,STM,CYBR,ON,GFS,"
        "U,RBRK,TWLO,OKTA,DT,GTLB,CFLT",
        validation_alias="TOP5_FIXED_POOL"
    )


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance, failing fast if required variables are missing."""
    try:
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        missing = ", ".join(str(err["loc"][0]) for err in exc.errors())
        msg = f"Configuration error. Missing or invalid settings: {missing}"
        raise RuntimeError(msg) from exc
