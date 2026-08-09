# Curated Feeds

`plugin_packages/curated_feeds/` 是可安装的主动内容源插件。它声明一个 stdio MCP server、一个 content
source 和随插件分发的 Drift skills。运行状态和本机配置位于：

```text
<workspace>/.kirakira/plugin-data/curated-feeds/
└── config.local.toml
```

## 支持的数据源

| kind | 用途 |
| --- | --- |
| `rss` / `atom` / `feed` | RSS 或 Atom 订阅 |
| `webpage` | 按正文 hash 监控页面变化 |
| `wordpress` | WordPress REST collection |
| `yahoo_market` | 按阈值或区间产生市场变化事件 |

`urls` 可以提供有序的 fallback 地址。市场源不会把每次报价轮询都变成新事件，只在配置的变化条件命中时
产生候选。

## 配置示例

```toml
[[feeds]]
id = "robot-news"
name = "Robot News"
kind = "rss"
url = "https://example.com/feed.xml"
topic = "robotics"
max_items = 6

[[feeds]]
id = "company-careers"
name = "Company Careers"
kind = "webpage"
url = "https://example.com/careers"
topic = "hiring"
```

实际字段以插件中的配置解析器为准。密钥、私有 bridge 地址和个人订阅应放在 `config.local.toml`，不要提交
到仓库。

## ACK 与 feedback

插件 source 使用稳定 event ID。内容摄入后 ACK；被模型引用或跳过后回传 feedback，供 source 调整后续
候选。投递失败不会错误消费事件。

## 维护原则

本文不维护会快速过期的公司名单、招聘状态或融资新闻。具体订阅目标属于用户配置；需要加入新源时先验证
URL 和解析格式，再写入本地配置。
