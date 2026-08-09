"""协议方法的参数契约。

Reference 用 pydantic 的 ``StrictModel``(``extra="forbid"`` + ``strict=True``)。
kirakira 全项目没有 pydantic 依赖(工具 schema 也是手写的 ``object_schema``),
为一个文件引入它不划算,因此这里手写等价校验器。

**线上契约与 Reference 逐字一致**:方法名、字段名、默认值、长度/范围约束、
未知字段拒绝、以及"不做类型强转"这一条都保持不变;不同的只是校验器实现。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


class ParamValidationError(Exception):
    """参数不合法。``issues`` 直接投影到 JSON-RPC error data。"""

    def __init__(self, issues: list[dict[str, Any]]) -> None:
        super().__init__("Invalid params")
        self.issues = issues


@dataclass(frozen=True)
class Field:
    kind: type | tuple[type, ...]
    required: bool = True
    default: Any = None
    default_factory: Callable[[], Any] | None = None
    min_length: int | None = None
    max_length: int | None = None
    ge: int | None = None
    le: int | None = None
    literal: tuple[Any, ...] | None = None
    nullable: bool = False

    def build_default(self) -> Any:
        if self.default_factory is not None:
            return self.default_factory()
        return self.default


@dataclass(frozen=True)
class ParamSpec:
    """一组具名字段。校验成功返回补齐默认值后的普通 dict。"""

    fields: dict[str, Field] = field(default_factory=dict)

    def validate(self, raw: dict[str, Any]) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []

        # 1. extra="forbid":未知字段是错误,不是被忽略。
        for name in raw:
            if name not in self.fields:
                issues.append(
                    {"loc": [name], "type": "extra_forbidden", "msg": "Extra inputs are not permitted"}
                )

        values: dict[str, Any] = {}
        for name, spec in self.fields.items():
            if name not in raw:
                if spec.required:
                    issues.append(
                        {"loc": [name], "type": "missing", "msg": "Field required"}
                    )
                else:
                    values[name] = spec.build_default()
                continue
            value = raw[name]
            if value is None and spec.nullable:
                values[name] = None
                continue
            problem = _check(value, spec)
            if problem is not None:
                issues.append({"loc": [name], "type": problem[0], "msg": problem[1]})
                continue
            values[name] = value

        if issues:
            raise ParamValidationError(issues)
        return values


def _check(value: Any, spec: Field) -> tuple[str, str] | None:
    # strict=True:bool 是 int 的子类,但绝不当成 int 接受。
    if spec.kind is int and isinstance(value, bool):
        return ("int_type", "Input should be a valid integer")
    if not isinstance(value, spec.kind):
        name = spec.kind.__name__ if isinstance(spec.kind, type) else "value"
        return (f"{name}_type", f"Input should be a valid {name}")
    if spec.literal is not None and value not in spec.literal:
        return ("literal_error", f"Input should be one of {list(spec.literal)}")
    if isinstance(value, str):
        if spec.min_length is not None and len(value) < spec.min_length:
            return ("string_too_short", f"String should have at least {spec.min_length} characters")
        if spec.max_length is not None and len(value) > spec.max_length:
            return ("string_too_long", f"String should have at most {spec.max_length} characters")
    if isinstance(value, int) and not isinstance(value, bool):
        if spec.ge is not None and value < spec.ge:
            return ("greater_than_equal", f"Input should be greater than or equal to {spec.ge}")
        if spec.le is not None and value > spec.le:
            return ("less_than_equal", f"Input should be less than or equal to {spec.le}")
    return None


_CLIENT_INFO = ParamSpec(
    {
        "name": Field(str, min_length=1, max_length=128),
        "version": Field(str, min_length=1, max_length=64),
    }
)
_CLIENT_CAPABILITIES = ParamSpec({"reasoningEvents": Field(bool, required=False, default=False)})


class InitializeSpec(ParamSpec):
    """initialize 额外校验两个嵌套 object。"""

    def validate(self, raw: dict[str, Any]) -> dict[str, Any]:
        values = super().validate(raw)
        try:
            values["clientInfo"] = _CLIENT_INFO.validate(dict(values["clientInfo"]))
        except ParamValidationError as exc:
            raise ParamValidationError(
                [{**issue, "loc": ["clientInfo", *issue["loc"]]} for issue in exc.issues]
            ) from exc
        try:
            values["capabilities"] = _CLIENT_CAPABILITIES.validate(
                dict(values["capabilities"])
            )
        except ParamValidationError as exc:
            raise ParamValidationError(
                [{**issue, "loc": ["capabilities", *issue["loc"]]} for issue in exc.issues]
            ) from exc
        return values


_THREAD_ID = {"threadId": Field(str, min_length=1, max_length=512)}

METHOD_PARAMS: dict[str, ParamSpec] = {
    "initialize": InitializeSpec(
        {
            "protocolVersion": Field(str, literal=("1.0",)),
            "clientInfo": Field(dict),
            "capabilities": Field(dict, required=False, default_factory=dict),
            "workspaceToken": Field(str, required=False, default=None, nullable=True),
        }
    ),
    "server/status": ParamSpec({}),
    "thread/start": ParamSpec({"metadata": Field(dict, required=False, default_factory=dict)}),
    "thread/resume": ParamSpec(dict(_THREAD_ID)),
    "thread/list": ParamSpec(
        {
            "cursor": Field(str, required=False, default=None, nullable=True),
            "limit": Field(int, required=False, default=50, ge=1, le=200),
        }
    ),
    "thread/read": ParamSpec(
        {**_THREAD_ID, "includeTurns": Field(bool, required=False, default=False)}
    ),
    "thread/delete": ParamSpec(dict(_THREAD_ID)),
    "thread/consolidate/start": ParamSpec(dict(_THREAD_ID)),
    "turn/start": ParamSpec(
        {
            **_THREAD_ID,
            "input": Field(str, min_length=1, max_length=1_048_576),
            "metadata": Field(dict, required=False, default_factory=dict),
        }
    ),
    "turn/read": ParamSpec(
        {**_THREAD_ID, "turnId": Field(str, min_length=1, max_length=128)}
    ),
    "turn/interrupt": ParamSpec(
        {**_THREAD_ID, "turnId": Field(str, min_length=1, max_length=128)}
    ),
    "plugin/disable-and-drain": ParamSpec(
        {"pluginId": Field(str, min_length=1, max_length=256)}
    ),
}


@dataclass(frozen=True)
class InitializeParams:
    """initialize 的 typed 视图,供 ControlService 消费。"""

    protocolVersion: str
    clientInfo: dict[str, Any]
    capabilities: dict[str, Any]
    workspaceToken: str | None = None

    @classmethod
    def from_values(cls, values: dict[str, Any]) -> InitializeParams:
        return cls(
            protocolVersion=values["protocolVersion"],
            clientInfo=values["clientInfo"],
            capabilities=values["capabilities"],
            workspaceToken=values.get("workspaceToken"),
        )
