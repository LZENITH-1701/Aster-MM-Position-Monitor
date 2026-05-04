# Aster Mid-Price Signal Monitor

实时监控 Aster Perp 上同时也在 Binance USDT Perp 的 ticker，捕捉「成交价长期贴近 mid-price + OI 持续抬升」的做市商静默建仓信号。

## 模块

| 文件 | 模块 |
|------|------|
| [monitor.py](monitor.py) | 主入口 + 任务编排 |
| [config.py](config.py) | 环境变量加载 |
| [symbols.py](symbols.py) | Binance ∩ Aster symbol 交集 |
| [ws_manager.py](ws_manager.py) | Aster WebSocket 行情订阅 |
| [oi_poller.py](oi_poller.py) | OI REST 轮询 |
| [signal_engine.py](signal_engine.py) | 滑动窗口信号检测 |
| [alert.py](alert.py) | 控制台 + Telegram 告警 |

## 本地运行

```bash
cd "做市商Aster静默建仓监控"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 不配 Telegram 也能跑，触发的信号会打到 stdout
python monitor.py
```

如需 Telegram，先 `cp .env.example .env` 并填好 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`，然后：

```bash
set -a; source .env; set +a
python monitor.py
```

## Heroku 部署

```bash
heroku create aster-midprice-monitor
heroku stack:set heroku-22  # 或 heroku-24

# 配置变量
heroku config:set TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=xxx
# 其他参数按需，不设就走默认

git init && git add -A && git commit -m "init"
heroku git:remote -a aster-midprice-monitor
git push heroku main

heroku ps:scale worker=1 web=0
heroku logs --tail
```

## 关键参数（详见 [.env.example](.env.example)）

| 参数 | 默认 | 说明 |
|------|------|------|
| `MIDPRICE_TOLERANCE` | 0.35 | last price 偏离 mid 的最大比例（占 spread） |
| `SIGNAL_WINDOW_SEC` | 1200 | 滑动窗口（秒） |
| `MIDPRICE_HIT_RATIO` | 0.75 | 窗口内贴近 mid 的笔数占比阈值 |
| `OI_INCREASE_RATIO` | 0.02 | 窗口期内 OI 最低增长比例 |
| `MIN_TRADES_IN_WINDOW` | 30 | 窗口内最少成交笔数 |
| `ALERT_COOLDOWN_SEC` | 600 | 同 ticker 告警冷却 |
| `WS_BATCH_SIZE` | 80 | 每条 WS 连接最多 symbol 数 |

## 信号判定流程

每笔 aggTrade 到达时：

1. 用当前 bid1/ask1 计算 `mid` 和 `spread`
2. 判定 `|trade_price - mid| <= spread * MIDPRICE_TOLERANCE`
3. 滑动窗口统计 `hit_ratio`
4. 同窗口取 OI 首尾算 `oi_change`
5. 同时满足阈值且过冷却，发告警

## 实测验证清单（来自计划）

- [ ] Aster 是否有 `/fapi/v1/openInterest`（启动时已自动探测，没有会降级到 `/premiumIndex`）
- [ ] Aster symbol 命名是否和 Binance 完全一致（启动日志会打印交集大小）
- [ ] Aster combined stream 行为（运行起来若收不到行情看日志里 `WS[*]` 部分）
- [ ] aggTrade 字段格式（异常会在 DEBUG 级别打印）

跑一段时间后照 `aster_midprice_monitor_plan.md` 第八节调参。
