"""插件类注册表(照 Reference `agent/plugins/registry.py` 的 class/instance 部分)。

插件类定义即注册(`Plugin.__init_subclass__` 调用),loader 因此不必靠类名猜测入口;
按模块路径索引,同一模块重载时覆盖旧类,配合热重载换代。

Reference 的 PluginHandlerMetadata 中央 handler 注册在 kirakira 用另一种等价方案:
装饰器把 binding 直接挂在函数上(`plugin_decorators.get_bindings`),manager 扫描实例方法。
两者语义相同,故此处只移植类/实例注册。
"""

from __future__ import annotations


class PluginRegistry:
    def __init__(self) -> None:
        self._classes: dict[str, type] = {}
        self._instances: dict[str, object] = {}

    def register_class(self, cls: type) -> None:
        self._classes[cls.__module__] = cls

    def register_instance(self, module_path: str, instance: object) -> None:
        self._instances[module_path] = instance

    def get_class(self, module_path: str) -> type | None:
        return self._classes.get(module_path)

    def get_instance(self, module_path: str) -> object | None:
        return self._instances.get(module_path)

    def forget(self, module_path: str) -> None:
        self._classes.pop(module_path, None)
        self._instances.pop(module_path, None)

    @property
    def module_paths(self) -> tuple[str, ...]:
        return tuple(sorted(self._classes))


plugin_registry = PluginRegistry()
