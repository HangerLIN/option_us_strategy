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
        Decimal("1.10"), validation_alias="IV_OVERNIGHT_CALL_CAP"
    )
    preearn_days_min: int = Field(3, validation_alias="PRE_EARN_DAYS_MIN")
    preearn_days_max: int = Field(5, validation_alias="PRE_EARN_DAYS_MAX")
    preearn_atr_pct_max: Decimal = Field(
        Decimal("0.02"), validation_alias="PRE_EARN_ATR_PCT_MAX"
    )
    risk_service_url: str = Field("http://localhost:8082", validation_alias="RISK_SERVICE_URL")
    option_bar_table: str = Field("bars1m_option", validation_alias="OPTION_BAR_TABLE")
    option_chain_table: str = Field("option_chain_meta", validation_alias="OPTION_CHAIN_TABLE")
    option_strike_lower_pct: Decimal = Field(Decimal("0.5"), validation_alias="OPTION_STRIKE_LOWER_PCT")
    option_strike_upper_pct: Decimal = Field(Decimal("1.5"), validation_alias="OPTION_STRIKE_UPPER_PCT")
    hist_backfill_window_min: int = Field(15, validation_alias="HIST_BACKFILL_WINDOW_MIN")
    
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
