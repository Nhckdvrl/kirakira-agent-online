# 0001 Phase slot 排序采用全员声明制

- 状态：accepted
- 范围：plugin lifecycle phase ordering

## 背景

Phase 模块需要表达依赖顺序，但旧插件只依赖注册顺序。如果部分模块声明 slot、部分没有，直接拓扑排序会
让未改动插件也发生隐式重排。

## 决定

只有某个 phase 的全部模块都声明 slot 时，才启用 dependency DAG 排序。只要存在未声明模块，该 phase
保持原注册/priority 顺序。

slot 重复、缺依赖或成环会记录错误并采用安全行为，不能打挂整个 runtime。

## 影响

存量插件可以逐步迁移；全员声明后自动获得拓扑排序，无需再开全局配置。代价是迁移期间不能只让一部分
模块享受 slot 排序，这是为了保持可预测性。
