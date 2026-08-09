"""主动链路配置。

对应 config.toml 的 ``[proactive]`` / ``[proactive.target]`` /
``[proactive.agent]`` / ``[proactive.drift]``，字段照 akashic 参考精简。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from agent.config import config_value


@dataclass
class DriftConfig:
    """Drift 空闲任务链路配置。"""

    enabled: bool = False
    min_interval_hours: float = 3.0
    max_steps: int = 20


@dataclass
class ProactiveConfig:
    """主动推送链路配置。"""

    enabled: bool = False
    channel: str = ""
    chat_id: str = ""
    content_limit: int = 5
    delivery_cooldown_hours: float = 1.0
    # 未读 content 超过该龄期（天）淘汰，防止队列无界增长；<=0 关闭
    content_max_age_days: float = 14.0
    # base_score 高（长期沉默或近期语境丰富）用短间隔，否则用长间隔。
    tick_interval_s1: int = 2400
    tick_interval_s0: int = 4800
    tick_jitter: float = 0.3
    model: str = ""
    max_tokens: int = 1024
    drift: DriftConfig = field(default_factory=DriftConfig)

    @property
    def session_key(self) -> str:
        return "%s:%s" % (self.channel, self.chat_id)

    @property
    def target_ready(self) -> bool:
        return bool(self.channel and self.chat_id)

    @classmethod
    def from_app_config(
        cls,
        app_config: Dict[str, Any],
        *,
        default_model: str = "",
    ) -> "ProactiveConfig":
        drift = DriftConfig(
            enabled=bool(
                config_value(app_config, "proactive", "drift", "enabled", default=False)
            ),
            min_interval_hours=float(
                config_value(
                    app_config, "proactive", "drift", "min_interval_hours", default=3.0
                )
            ),
            max_steps=int(
                config_value(app_config, "proactive", "drift", "max_steps", default=20)
            ),
        )
        return cls(
            enabled=bool(config_value(app_config, "proactive", "enabled", default=False)),
            channel=str(
                config_value(app_config, "proactive", "target", "channel", default="")
            ).strip(),
            chat_id=str(
                config_value(app_config, "proactive", "target", "chat_id", default="")
            ).strip(),
            content_limit=int(
                config_value(app_config, "proactive", "agent", "content_limit", default=5)
            ),
            delivery_cooldown_hours=float(
                config_value(
                    app_config,
                    "proactive",
                    "agent",
                    "delivery_cooldown_hours",
                    default=1.0,
                )
            ),
            content_max_age_days=float(
                config_value(
                    app_config,
                    "proactive",
                    "agent",
                    "content_max_age_days",
                    default=14.0,
                )
            ),
            tick_interval_s1=int(
                config_value(
                    app_config, "proactive", "tick_interval_s1", default=2400
                )
            ),
            tick_interval_s0=int(
                config_value(
                    app_config, "proactive", "tick_interval_s0", default=4800
                )
            ),
            tick_jitter=float(
                config_value(app_config, "proactive", "tick_jitter", default=0.3)
            ),
            model=str(
                config_value(app_config, "proactive", "model", default="")
            ).strip()
            or default_model,
            max_tokens=int(
                config_value(app_config, "proactive", "max_tokens", default=1024)
            ),
            drift=drift,
        )
