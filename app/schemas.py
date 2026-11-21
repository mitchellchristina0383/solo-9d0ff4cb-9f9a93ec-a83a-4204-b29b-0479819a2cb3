"""Pydantic request models with operator-friendly validation messages."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .analysis import BLOCKING_SUBSCRIPTION_STATES
from .lsn import format_lsn, parse_lsn

SlotType = Literal["logical", "physical"]
WalStatus = Literal["reserved", "extended", "unreserved", "lost"]
SubscriptionStatus = Literal["online", "stalled", "disabled"]

assert BLOCKING_SUBSCRIPTION_STATES == {"online", "stalled"}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


def _check_name(value: str, field: str) -> str:
    if not value:
        raise ValueError(f"{field} 不能为空")
    if len(value) > 64:
        raise ValueError(f"{field} 长度不能超过 64")
    if any(ch.isspace() for ch in value):
        raise ValueError(f"{field} 不能包含空白字符")
    return value


def _lsn_value(value: str) -> str:
    return format_lsn(parse_lsn(value))


class ClusterUpdate(StrictModel):
    wal_boundary_lsn: str = Field(description="当前 WAL 边界，例如 0/2000000")

    @field_validator("wal_boundary_lsn")
    @classmethod
    def _validate(cls, value: str) -> str:
        return _lsn_value(value)


class SlotUpsert(StrictModel):
    name: str
    slot_type: SlotType
    restart_lsn: str
    confirmed_flush_lsn: str | None = None
    active: bool = False
    disabled: bool = False
    wal_status: WalStatus = "reserved"
    plugin: str | None = None
    note: str | None = None

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        return _check_name(value, "槽名")

    @field_validator("restart_lsn", "confirmed_flush_lsn")
    @classmethod
    def _lsn(cls, value: str | None) -> str | None:
        return _lsn_value(value) if value is not None else None

    @field_validator("plugin", "note")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        return value or None

    def model_post_init(self, __context: object) -> None:
        if self.active and self.disabled:
            raise ValueError("槽不能同时为 active 和 disabled")
        if self.slot_type == "logical" and self.confirmed_flush_lsn is None:
            raise ValueError("logical 槽必须提供 confirmed_flush_lsn")
        if self.slot_type == "physical" and self.confirmed_flush_lsn is not None:
            raise ValueError("physical 槽不能提供 confirmed_flush_lsn")


class SlotPatch(StrictModel):
    """Partial update; omitted fields keep their current value."""

    slot_type: SlotType | None = None
    restart_lsn: str | None = None
    confirmed_flush_lsn: str | None = None
    active: bool | None = None
    disabled: bool | None = None
    wal_status: WalStatus | None = None
    plugin: str | None = None
    note: str | None = None

    @field_validator("restart_lsn", "confirmed_flush_lsn")
    @classmethod
    def _lsn(cls, value: str | None) -> str | None:
        return _lsn_value(value) if value is not None else None


class SubscriptionUpsert(StrictModel):
    name: str
    slot_name: str | None = None
    status: SubscriptionStatus = "online"
    confirmed_lsn: str
    note: str | None = None

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        return _check_name(value, "订阅名")

    @field_validator("confirmed_lsn")
    @classmethod
    def _lsn(cls, value: str) -> str:
        return _lsn_value(value)

    @field_validator("slot_name", "note")
    @classmethod
    def _optional(cls, value: str | None) -> str | None:
        return value or None

    def model_post_init(self, __context: object) -> None:
        if self.status != "disabled" and self.slot_name is None:
            raise ValueError("非 disabled 订阅必须绑定 slot_name")
        if self.status == "disabled" and self.slot_name is not None:
            raise ValueError("disabled 订阅必须解除 slot_name 绑定（传 null）")


class SubscriptionPatch(StrictModel):
    slot_name: str | None = None
    status: SubscriptionStatus | None = None
    confirmed_lsn: str | None = None
    note: str | None = None

    @field_validator("confirmed_lsn")
    @classmethod
    def _lsn(cls, value: str | None) -> str | None:
        return _lsn_value(value) if value is not None else None


class PauseSubscriptionRequest(StrictModel):
    note: str | None = None


class PreviewRequest(StrictModel):
    slot_names: list[str] = Field(min_length=1, max_length=100, description="待清理槽名列表")
    note: str | None = Field(default=None, max_length=500)

    @field_validator("slot_names")
    @classmethod
    def _names(cls, value: list[str]) -> list[str]:
        cleaned = [_check_name(v, "槽名") for v in value]
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("slot_names 不能重复")
        return cleaned

    @field_validator("note")
    @classmethod
    def _note(cls, value: str | None) -> str | None:
        return value or None


class PreviewConfirmRequest(StrictModel):
    """Optimistic concurrency: the operator must echo the preview fingerprint."""

    fingerprint: str = Field(min_length=64, max_length=64)
    note: str | None = Field(default=None, max_length=500)

    @field_validator("fingerprint")
    @classmethod
    def _fp(cls, value: str) -> str:
        lowered = value.lower()
        if any(ch not in "0123456789abcdef" for ch in lowered):
            raise ValueError("fingerprint 必须是 64 位十六进制 SHA-256")
        return lowered

    @field_validator("note")
    @classmethod
    def _note(cls, value: str | None) -> str | None:
        return value or None
