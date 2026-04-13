"""LLM model configuration (SDD §10)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class LLMConfig:
    model: str
    max_tokens: int = 4096
    temperature: float = 0.0

    @classmethod
    def from_env(cls, override_model: str | None = None) -> "LLMConfig":
        return cls(
            model=override_model
            or os.environ.get("DEFAULT_LLM_MODEL", "claude-sonnet-4-20250514"),
            max_tokens=int(os.environ.get("DEFAULT_LLM_MAX_TOKENS", "4096")),
        )
