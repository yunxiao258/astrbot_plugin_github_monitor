# astrbot_plugin_github_monitor

AstrBot 插件：监控 GitHub 仓库更新（提交 / Release / 标签 / Issue / PR），支持 Star 趋势追踪与每日趋势日报，自动推送到群聊与私聊。

版本：v1.3.0 | 许可证：MIT

## 功能特性

- **定时轮询**：默认每 5 分钟自动检查一次，发现更新立即推送（可配置，也可手动 `/gh check`）
- **多类型更新**：支持检测新提交、新 Release、新标签、新 Issue、新 PR
- **富信息推送**：包含 SHA、提交信息、作者、时间、变更行数统计、链接等
- **动态仓库管理**：`/gh add` / `/gh remove` 随时增删监控仓库，重启不丢失
- **会话订阅**：`/gh 订阅` / `/gh 退订` 控制每个群/私聊是否接收推送；使用过指令的会话自动订阅
- **个人仓库列表**：`/gh repos` 一键查看你的仓库
- **Token 管理**：`/gh settoken` 动态设置 GitHub Token（Windows 上 DPAPI 加密存储，仅本机可读），支持私有仓库与更高 API 限额
- **Star 趋势追踪**：随监控循环自动记录各仓库每日 Star 数（`trends.json`），`/gh star` 查看近 7 天块字符趋势图与 24h / 7d 变化数据
- **Star 增长提醒**：单仓库 24h 内 Star 增长达到阈值（`star_alert_threshold`）自动提醒订阅者，同一天只提醒一次
- **每日趋势日报**：汇总各订阅仓库昨日的提交 / 发布 / 标签 / Issue / PR 计数与 Star 变化，支持手动发送（`/gh daily`）与定时自动发送（默认 09:00，北京时间）
- **智能过滤**：关键词过滤（`keyword_filters`）、Issue/PR 标签过滤（`issue_tags` / `pull_tags`）、星标数下限过滤（`min_stars`）
- **AI 摘要**：可选启用（`enable_ai_summary`），用默认 LLM Provider 为新提交 / 发布 / Issue / PR 生成一句话摘要
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
| `/gh star owner/repo` | 查看近 7 天 Star 趋势图与 24h / 7d 变化（仓库需已在监控列表） |
| `/gh daily` | 手动发送每日趋势日报（汇总各仓库昨日数据） |
| `/gh settoken <token>` | 设置 GitHub Token（验证有效性后保存） |
| `/gh 订阅` | 本会话订阅推送 |
| `/gh 退订` | 本会话退订推送 |

> 提示 1：`/gh add` 添加仓库后首次仅建立基线（不会立即通知），从之后开始的更新才会推送。
> 提示 2：`/gh star` 需要仓库已在监控列表中（先 `/gh add`），且运行一段时间积累历史数据后才能显示完整趋势；`/gh daily` 手动发送后当日定时任务不再重复推送。

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
| `keyword_filters` | 空 | 关键词过滤：提交信息 / 发布说明命中任一关键词才推送（逗号或顿号分隔，留空不过滤） |
| `min_stars` | `0` | 星标数下限：Star 数低于此值的仓库不推送更新（0 为不限） |
| `enable_ai_summary` | `false` | 是否启用 AI 摘要（用默认 LLM Provider 生成一句话摘要） |
| `star_alert_threshold` | `10` | Star 24h 增长提醒阈值：单仓库一天内 Star 增长达到该值（颗）时提醒订阅者，0 为不提醒 |
| `issue_tags` | 空 | Issue 标签过滤：只推送带这些标签的新 Issue（逗号或顿号分隔，留空推送全部） |
| `pull_tags` | 空 | PR 标签过滤：只推送带这些标签的新 Pull Request（逗号或顿号分隔，留空推送全部） |
| `daily_report_enabled` | `true` | 是否启用每日趋势日报定时发送 |
| `daily_report_time` | `09:00` | 每日趋势日报发送时间（24 小时制，如 `09:00`，非法值回退 09:00） |

## 数据存储

状态保存在插件数据目录 `data/plugins/astrbot_plugin_github_monitor/plugin_data/astrbot_plugin_github_monitor/` 下：

| 文件 | 内容 |
| --- | --- |
| `state.json` | Token（加密存储）、监控仓库、订阅会话、更新基线 |
| `trends.json` | 各仓库按日期的 Star 历史与提醒状态（自动保留最近 30 天） |
| `daily_stats.json` | 各仓库按日期的更新计数（提交 / 发布 / 标签 / Issue / PR）与最近日报发送日期（自动保留最近 30 天） |

## 依赖

- 使用 `aiohttp`（AstrBot 环境自带）
- 网络需可访问 `api.github.com`（如遇网络问题可配置代理）
- AI 摘要功能需要 AstrBot 已配置可用的 LLM Provider

## 常见问题

- **添加私有仓库提示不存在**：需先用 `/gh settoken` 配置有效 Token
- **`/gh repos` 提示限流**：未配置 Token 时受 GitHub 匿名限额（60 次/小时）限制，配置 Token 后可提升至 5000 次/小时
- **收不到推送**：检查是否订阅（`/gh 订阅`），确认插件已启用且监控列表非空
- **`/gh star` 提示无历史数据**：Star 趋势需要插件运行后按天积累，刚添加的仓库或运行不足两天的仓库暂无 24h 变化数据
- **每日日报提示无数据**：日报数据来自每日自动检查的记录，插件在昨日未运行或未检查到任何仓库时无法生成有效日报

## 更新记录

### v1.3.0

- 新增 Star 趋势追踪：自动记录每日 Star 数，`/gh star` 查看近 7 天趋势图与 24h / 7d 变化，`star_alert_threshold` 支持 24h 增长阈值提醒
- 新增每日趋势日报：`/gh daily` 手动发送，`daily_report_time` 定时自动发送，汇总各仓库昨日更新计数与 Star 变化
- 新增过滤能力：`keyword_filters` 关键词过滤、`issue_tags` / `pull_tags` 标签过滤、`min_stars` 星标数下限过滤
- 新增 `enable_ai_summary`：用默认 LLM Provider 为更新条目生成一句话摘要
- Token 改为 Windows DPAPI 加密存储，不再明文落盘