# astrbot_plugin_github_monitor

AstrBot 插件：监控 GitHub 仓库更新（提交 / Release / 标签），自动推送到群聊与私聊。

## 功能特性

- **定时轮询**：默认每 5 分钟自动检查一次，发现更新立即推送（可配置，也可手动 `/gh check`）
- **多类型更新**：支持检测新提交、新 Release、新标签、新 Issue、新 PR
- **富信息推送**：包含 SHA、提交信息、作者、时间、变更行数统计、链接等
- **动态仓库管理**：`/gh add` / `/gh remove` 随时增删监控仓库，重启不丢失
- **会话订阅**：`/gh 订阅` / `/gh 退订` 控制每个群/私聊是否接收推送；使用过指令的会话自动订阅
- **个人仓库列表**：`/gh repos` 一键查看你的仓库
- **Token 管理**：`/gh settoken` 动态设置 GitHub Token（仅本机存储，不上传），支持私有仓库与更高 API 限额
- **重启补偿**：状态持久化，重启后不会重复推送已通知过的更新

## 安装

将本目录放入 AstrBot 的 `data/plugins/` 下，重启 AstrBot 或在「插件管理」中启用。

## 使用说明

| 指令 | 说明 |
| --- | --- |
| `/gh help` | 查看帮助 |
| `/gh add owner/repo` | 添加监控仓库（支持完整 URL / `.git` 后缀） |
| `/gh remove owner/repo` | 移除监控仓库 |
| `/gh list` | 查看当前监控列表及状态 |
| `/gh repos` | 查看个人仓库列表（需 Token 或默认账号） |
| `/gh check [owner/repo]` | 立即检查更新（不指定则检查全部） |
| `/gh settoken <token>` | 设置 GitHub Token（验证有效性后保存） |
| `/gh 订阅` | 本会话订阅推送 |
| `/gh 退订` | 本会话退订推送 |

> 提示：`/gh add` 添加仓库后首次仅建立基线（不会立即通知），从之后开始的更新才会推送。

## 配置项（`_conf_schema.json`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enable_auto_check` | `true` | 是否启用定时自动检查 |
| `check_interval_minutes` | `5` | 自动检查间隔（分钟） |
| `github_token` | 空 | GitHub Token（也可用 `/gh settoken` 设置） |
| `default_repos` | `yunxiao258/astrbot_plugin_context_analyzer` | 默认监控仓库，逗号分隔 |
| `default_owner` | `yunxiao258` | 未配置 Token 时 `/gh repos` 查询的用户 |
| `default_subscribers` | 空 | 默认订阅会话（`unified_msg_origin`），逗号分隔 |
| `max_events_per_check` | `5` | 每仓库每次最多通知的更新条数 |
| `show_commit_stats` | `true` | 是否显示提交变更行数统计（会额外请求 API） |
| `notify_release` | `true` | 是否通知新 Release |
| `notify_issue` | `true` | 是否通知新 Issue |
| `notify_pr` | `true` | 是否通知新 PR |
| `notify_tag` | `true` | 是否通知新标签 |

## 数据存储

状态（Token、监控仓库、订阅会话、更新基线）保存在插件数据目录 `data/plugins/astrbot_plugin_github_monitor/plugin_data/astrbot_plugin_github_monitor/state.json`。

## 依赖

- 使用 `aiohttp`（AstrBot 环境自带）
- 网络需可访问 `api.github.com`（如遇网络问题可配置代理）

## 常见问题

- **添加私有仓库提示不存在**：需先用 `/gh settoken` 配置有效 Token
- **`/gh repos` 提示限流**：未配置 Token 时受 GitHub 匿名限额（60 次/小时）限制，配置 Token 后可提升至 5000 次/小时
- **收不到推送**：检查是否订阅（`/gh 订阅`），确认插件已启用且监控列表非空
