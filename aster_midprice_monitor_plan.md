# Aster Mid-Price Signal Monitor — 项目计划

## 一、项目背景与信号定义

### 信号描述
在 Aster Perp 上，当某个 ticker 的 **last price 长期（~20分钟）贴近 bid1/ask1 的中间价（mid-price）**，且 **OI 持续增长**，判定为做市商静默建仓信号，做多。

### 适用范围
所有同时上了 **Binance USDT Perp** 和 **Aster Perp** 的 ticker。

---

## 二、模块拆解

### 模块 1：Symbol 交集获取

**目标**：自动获取 Binance Perp ∩ Aster Perp 的 symbol 列表。

**数据源**：
- Binance: `GET https://fapi.binance.com/fapi/v1/exchangeInfo`
  - 过滤条件: `contractType == "PERPETUAL"` & `quoteAsset == "USDT"` & `status == "TRADING"`
  - 提取: `baseAsset`
- Aster: `GET https://fapi.asterdex.com/fapi/v1/exchangeInfo`
  - 过滤条件: `contractType == "PERPETUAL"` & `quoteAsset == "USDT"` & `status == "TRADING"`
  - 提取: `baseAsset`

**逻辑**：
1. 两边各拉一次 exchangeInfo
2. 提取 baseAsset 做集合交集
3. 输出格式: `["TOKENAUSDT", "TOKENBUSDT", ...]`
4. 定时刷新（每小时或每次启动时）

**注意**：
- Aster 的 symbol 命名可能和 Binance 不完全一致（需要实测确认）
- 交集结果需要 log 输出，方便排查

---

### 模块 2：Aster 盘口数据获取

**目标**：实时获取每个监控 symbol 的 bid1、ask1、mid-price、last price。

**数据源（Aster API）**：

| 数据点 | 获取方式 | 端点/Stream |
|--------|---------|------------|
| **Bid1 价格** | WebSocket | `<symbol>@bookTicker` → `bidPrice` |
| **Bid1 数量** | WebSocket | `<symbol>@bookTicker` → `bidQty` |
| **Ask1 价格** | WebSocket | `<symbol>@bookTicker` → `askPrice` |
| **Ask1 数量** | WebSocket | `<symbol>@bookTicker` → `askQty` |
| **Mid-Price** | 本地计算 | `(bid1 + ask1) / 2` |
| **Last Price** | WebSocket | `<symbol>@aggTrade` → `p` (price) |
| **Spread** | 本地计算 | `ask1 - bid1` |

**WebSocket 配置**：
- Base URL: `wss://fstream.asterdex.com`
- Combined stream: `/stream?streams=<stream1>/<stream2>/...`
- 每个 symbol 需要 2 个 stream: `@bookTicker` + `@aggTrade`
- 单连接最多 200 个 stream → 最多监控 100 个 symbol/连接
- Stream 名全小写

**REST 备选**（用于初始化/降级）：
- bookTicker: `GET https://fapi.asterdex.com/fapi/v1/ticker/bookTicker?symbol=XXXUSDT`
- lastPrice: `GET https://fapi.asterdex.com/fapi/v1/ticker/price?symbol=XXXUSDT`

---

### 模块 3：OI 数据获取

**目标**：定时获取每个监控 symbol 的 Open Interest。

**数据源**：
- Aster REST: `GET https://fapi.asterdex.com/fapi/v1/openInterest?symbol=XXXUSDT`
  - 返回: `{ "openInterest": "10000.000", "symbol": "XXXUSDT", "time": 1700000000000 }`
  - **注意**：此端点是否存在需要实测验证（Aster fork 自 Binance，大概率有）
- 备选: 从 `GET /fapi/v1/premiumIndex` 获取（如果 openInterest 端点不存在）

**轮询策略**：
- 每 30 秒轮询一次所有 symbol
- 每次请求间隔 100ms（避免触发限速）
- 记录 (timestamp, oi_value) 到内存队列

---

### 模块 4：信号检测引擎

**目标**：滑动窗口判定信号条件。

**数据结构**（每个 symbol）：
```
TickerState:
  - best_bid: float          # 买1
  - best_ask: float          # 卖1  
  - trade_hits: deque        # [(timestamp, is_near_mid: bool), ...]
  - oi_history: deque        # [(timestamp, oi_value), ...]
  - last_alert_ts: float     # 上次告警时间（冷却用）
```

**判定逻辑**：
1. 每笔成交到达时：
   - 计算 mid = (bid1 + ask1) / 2
   - 计算 spread = ask1 - bid1
   - 判定: `|trade_price - mid| <= spread * TOLERANCE`
   - 记录 (now, True/False) 到 trade_hits
2. 滑动窗口统计（窗口 = SIGNAL_WINDOW_SEC）：
   - 清理窗口外数据
   - hit_ratio = 贴近mid的笔数 / 总笔数
3. OI 趋势：
   - 取窗口内 OI 首尾值
   - oi_change = (oi_end - oi_start) / oi_start
4. 触发条件：
   - `hit_ratio >= MIDPRICE_HIT_RATIO` **且** `oi_change >= OI_INCREASE_RATIO`
   - 同一 symbol 冷却期内不重复告警

---

### 模块 5：告警输出

**目标**：信号触发时推送通知。

**输出渠道**（按优先级）：
1. **Telegram Bot**（首选）
   - 需要: BOT_TOKEN + CHAT_ID
   - 消息内容: symbol、hit_ratio、OI变化%、当前price、时间戳
2. **控制台日志**（必须有）
   - 结构化日志输出
3. **Webhook**（可选扩展）

**告警消息模板**：
```
🚨 ASTER SIGNAL: {SYMBOL}
━━━━━━━━━━━━━━━━━━━━
Mid-price hit ratio: {hit_ratio}%
OI change: +{oi_change}%
OI: {oi_start} → {oi_end}
Current spread: {spread}
Mid-price: {mid}
Window trades: {count}
Time: {timestamp}
━━━━━━━━━━━━━━━━━━━━
```

---

### 模块 6：部署（Heroku）

**运行方式**：Python worker dyno（非 web dyno）

**所需文件**：
- `Procfile`: `worker: python monitor.py`
- `requirements.txt`
- `runtime.txt`: `python-3.11.x`

---

## 三、环境变量清单

| 环境变量名 | 说明 | 默认值 | 必须? |
|-----------|------|--------|------|
| `ASTER_REST_BASE` | Aster REST API base URL | `https://fapi.asterdex.com` | 否 |
| `ASTER_WS_BASE` | Aster WebSocket base URL | `wss://fstream.asterdex.com` | 否 |
| `BINANCE_REST_BASE` | Binance REST API base URL | `https://fapi.binance.com` | 否 |
| `MIDPRICE_TOLERANCE` | last price 偏离 mid 的最大比例（占 spread） | `0.35` | 否 |
| `SIGNAL_WINDOW_SEC` | 信号检测滑动窗口（秒） | `1200` (20分钟) | 否 |
| `MIDPRICE_HIT_RATIO` | 窗口内成交贴近 mid 的笔数占比阈值 | `0.75` | 否 |
| `OI_INCREASE_RATIO` | 窗口期内 OI 最低增长比例 | `0.02` (2%) | 否 |
| `OI_POLL_INTERVAL_SEC` | OI 轮询间隔（秒） | `30` | 否 |
| `MIN_TRADES_IN_WINDOW` | 窗口内最少成交笔数（统计有效性） | `30` | 否 |
| `ALERT_COOLDOWN_SEC` | 同一 ticker 告警冷却时间（秒） | `600` (10分钟) | 否 |
| `SYMBOL_REFRESH_INTERVAL_SEC` | Symbol 交集列表刷新间隔（秒） | `3600` (1小时) | 否 |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token | _(空)_ | 否* |
| `TELEGRAM_CHAT_ID` | Telegram Chat/Group ID | _(空)_ | 否* |
| `LOG_LEVEL` | 日志级别 | `INFO` | 否 |
| `WS_BATCH_SIZE` | 每个 WS 连接最大 symbol 数 | `80` | 否 |

> *TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID 不填则只输出控制台日志。

---

## 四、技术选型

| 组件 | 选择 | 原因 |
|------|------|------|
| 语言 | Python 3.11+ | asyncio 生态成熟，你熟悉 |
| WS 库 | `websockets` | 轻量、async native |
| HTTP 库 | `aiohttp` | async HTTP，配合 asyncio |
| 日志 | `logging` (stdlib) | 够用 |
| Telegram | `aiohttp` 直接调 API | 不需要引入 bot framework |
| 部署 | Heroku worker dyno | 你的现有基建 |

**依赖列表**：
```
websockets>=12.0
aiohttp>=3.9
```

---

## 五、文件结构

```
aster-midprice-monitor/
├── monitor.py              # 主入口 + 主循环
├── config.py               # 环境变量读取 + 默认值
├── symbols.py              # 模块1: Binance ∩ Aster symbol 交集
├── ws_manager.py           # 模块2: WebSocket 连接管理
├── oi_poller.py            # 模块3: OI 轮询
├── signal_engine.py        # 模块4: 信号检测引擎
├── alert.py                # 模块5: 告警输出（console + Telegram）
├── Procfile                # Heroku 部署
├── requirements.txt        # 依赖
├── runtime.txt             # Python 版本
└── README.md               # 使用说明
```

---

## 六、启动流程

```
1. 读取环境变量 → config
2. 调用 symbols.py 获取 Binance Perp ∩ Aster Perp 交集
3. 初始化每个 symbol 的 TickerState
4. 启动 asyncio 任务组:
   ├─ ws_manager: N 个 WS 连接（每个最多 WS_BATCH_SIZE 个 symbol）
   │   ├─ 订阅 @bookTicker → 更新 bid1/ask1
   │   └─ 订阅 @aggTrade → 记录成交 + 触发信号检测
   ├─ oi_poller: 定时 REST 轮询 OI
   └─ symbol_refresher: 定时刷新 symbol 列表（热更新）
5. 信号触发 → alert.py 输出
```

---

## 七、需要实测验证的点

1. **Aster 是否有 `/fapi/v1/openInterest` 端点**
   - 如果没有，备选方案：用 `@markPrice` stream 里是否携带 OI 字段，或从 on-chain 数据推算
2. **Aster symbol 命名是否和 Binance 完全一致**
   - 例如 Binance 是 `BNXUSDT`，Aster 是否也叫 `BNXUSDT`
3. **Aster WS combined stream 格式是否和 Binance 一致**
   - 文档显示一致，但需要实际连接测试
4. **Aster 的 aggTrade stream 数据格式**
   - 预期和 Binance 一致: `{ "p": price, "q": qty, "m": isBuyerMaker, ... }`

---

## 八、参数调优建议

| 参数 | 说明 | 调优方向 |
|------|------|---------|
| MIDPRICE_TOLERANCE | Alpha/Perp 标的 spread 宽，如果 MM 真的在 mid 挂单，这个值应该偏小 | 先用 0.35 观察，可能需要收紧到 0.2 |
| SIGNAL_WINDOW_SEC | 20分钟是经验值 | 可以尝试 15-30 分钟范围 |
| MIDPRICE_HIT_RATIO | 75% 是保守值 | 如果误报多就提高到 0.85 |
| OI_INCREASE_RATIO | 2% 作为起步 | 小币 OI 基数小，可能需要用绝对值而非比例 |
| MIN_TRADES_IN_WINDOW | 30 笔保底 | 如果某些币太冷清可能永远达不到，需要按币调整 |
