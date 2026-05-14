import asyncio
import ctypes
import hashlib
import hmac
import json
import math
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional
from urllib.parse import urlencode

import requests
import websockets
from PySide6.QtCore import QThread, Qt, Signal
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QDoubleSpinBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QRadioButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


# =========================
# 기본 Settings
# =========================
APP_NAME = "꼬라니 바이낸스 자동매매 1.02v"
APP_ICON_PATH = "app.ico"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "BNBUSDT"]
INTERVALS = ["15m", "30m", "1h", "4h", "12h", "1d"]

REST_BASE_URL = "https://fapi.binance.com"
WS_BASE_URL = "wss://fstream.binance.com/market"

RECV_WINDOW = 5000
CANDLE_LIMIT = 220
VOLUME_SPIKE_MULTIPLIER = 1.5
SYNC_COOLDOWN_SECONDS = 2.0

# =========================
# github Data
# =========================
GITHUB_USER_BASE_URL = "https://raw.githubusercontent.com/longscom90/CA_B/main"
REQUEST_TIMEOUT = 10


@dataclass
class Candle:
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool

    @property
    def body_low(self) -> float:
        return min(self.open, self.close)

    @property
    def body_high(self) -> float:
        return max(self.open, self.close)

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.open > self.close


@dataclass
class SymbolMeta:
    symbol: str
    quantity_precision: int
    price_precision: int
    step_size: float
    tick_size: float
    min_qty: float
    min_notional: float


def fetch_user_profile_from_github(user_id: str) -> dict:
    user_id = user_id.strip()
    if not user_id:
        raise ValueError("유저 ID가 비어 있습니다.")

    url = f"{GITHUB_USER_BASE_URL}/{user_id}.txt"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)

    if resp.status_code == 404:
        raise ValueError("존재하지 않는 유저 ID 입니다. 관리자에게 문의하세요.")

    resp.raise_for_status()

    lines = [line.strip() for line in resp.text.splitlines() if line.strip()]

    if len(lines) < 4:
        raise ValueError("유저 파일 형식이 올바르지 않습니다. 관리자에게 문의하세요.")

    return {
        "user_id": user_id,
        "auth_code": lines[0],
        "api_key": lines[1],
        "api_secret": lines[2],
        "discord_webhook": lines[3],
    }


class BinanceFuturesClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key.strip()
        self.api_secret_text = api_secret.strip()
        self.api_secret = self.api_secret_text.encode("utf-8")
        self.base_url = "https://demo-fapi.binance.com" if testnet else REST_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})

    def _sign(self, params: dict) -> str:
        qs = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"{qs}&signature={signature}"

    def _raise(self, resp: requests.Response):
        if resp.status_code >= 400:
            try:
                payload = resp.json()
            except Exception:
                payload = {"code": resp.status_code, "msg": resp.text}
            raise RuntimeError(f"Binance API error: {payload}")

    def public_get(self, path: str, params: Optional[dict] = None):
        resp = self.session.get(f"{self.base_url}{path}", params=params or {}, timeout=15)
        self._raise(resp)
        return resp.json()

    def signed_request(self, method: str, path: str, params: Optional[dict] = None):
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = RECV_WINDOW
        url = f"{self.base_url}{path}?{self._sign(params)}"
        resp = self.session.request(method, url, timeout=15)
        self._raise(resp)
        return resp.json()

    def get_exchange_info(self):
        return self.public_get("/fapi/v1/exchangeInfo")

    def get_klines(self, symbol: str, interval: str, limit: int = CANDLE_LIMIT):
        return self.public_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def set_position_mode_one_way(self):
        try:
            return self.signed_request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        except RuntimeError as e:
            if "-4059" in str(e) or "No need to change" in str(e):
                return {"msg": "already one-way"}
            raise

    def set_margin_type(self, symbol: str, margin_type: str):
        try:
            return self.signed_request("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})
        except RuntimeError as e:
            if "-4046" in str(e) or "No need to change" in str(e):
                return {"msg": "already set"}
            raise

    def set_leverage(self, symbol: str, leverage: int):
        return self.signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    def new_market_order(self, symbol: str, side: str, quantity: float, reduce_only: bool = False):
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "newOrderRespType": "RESULT",
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        return self.signed_request("POST", "/fapi/v1/order", params)

    def place_tp_sl_algo(self, symbol: str, side: str, quantity: float, tp_trigger: float, sl_trigger: float):
        tp = self.signed_request(
            "POST",
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": symbol,
                "side": side,
                "type": "TAKE_PROFIT_MARKET",
                "quantity": quantity,
                "triggerPrice": tp_trigger,
                "reduceOnly": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )
        sl = self.signed_request(
            "POST",
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": symbol,
                "side": side,
                "type": "STOP_MARKET",
                "quantity": quantity,
                "triggerPrice": sl_trigger,
                "reduceOnly": "true",
                "workingType": "CONTRACT_PRICE",
            },
        )
        return tp, sl

    def get_account_balance(self):
        return self.signed_request("GET", "/fapi/v2/balance")

    def get_position_risk(self):
        return self.signed_request("GET", "/fapi/v3/positionRisk")

    def close(self):
        self.session.close()


class TradingWorker(QThread):
    log_signal = Signal(str)
    status_signal = Signal(str)
    trade_signal = Signal(str)

    def __init__(self, config: dict):
        super().__init__()
        self.config = config
        self.running = False
        self.client: Optional[BinanceFuturesClient] = None
        self.closed_candles: Dict[str, Dict[str, Deque[Candle]]] = {}
        self.live_candle: Dict[str, Dict[str, Optional[Candle]]] = {}
        self.bullish_ob_active: Dict[str, Dict[str, bool]] = {}
        self.bullish_ob_time: Dict[str, Dict[str, Optional[int]]] = {}
        self.metas: Dict[str, SymbolMeta] = {}
        self.active_trade: dict = self._empty_trade()
        self.last_alerts: Dict[str, float] = {}
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.last_sync_ts = 0.0

    def _empty_trade(self) -> dict:
        return {
            "symbol": None,
            "interval": None,
            "entry_price": None,
            "tp_price": None,
            "sl_price": None,
            "qty": None,
            "is_active": False,
        }

    def log(self, text: str):
        self.log_signal.emit(text)

    def status(self, text: str):
        self.status_signal.emit(text)

    def stop(self):
        self.running = False
        self.status("중지 요청됨")
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(lambda: None)

    def format_kst(self, ts_ms: int) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000 + 9 * 3600))

    def send_discord(self, content: str):
        url = self.config["discord_webhook"].strip()
        if not url:
            return
        payload = {"content": content, "allowed_mentions": {"parse": []}}
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            self.log(f"[디스코드 오류] {e}")

    # =========================
    # 디스코드 알림
    # =========================
    def send_program_started_discord(self):
        msg = (
            "✅ 자동매매 프로그램이 실행 되었습니다.\n"
            f"📌 감시 종목: {', '.join(self.config['selected_symbols'])}\n"
            f"📌 감시 시간봉: {', '.join(INTERVALS)}\n"
            f"📌 청산 방식: {'자동 시장가 청산' if self.config['exit_mode'] == 'bot' else 'TP/SL 자동 등록'}\n"
            f"📌 주문 방식: {'USDT(직접입력)' if self.config['sizing_mode'] == 'fixed' else '계좌 잔고 비율(%)'}\n"
            "------------------------------------------------"
        )
        self.send_discord(msg)

    def send_trade_open_discord(
        self,
        symbol: str,
        interval: str,
        entry_price: float,
        tp_price: float,
        sl_price: float,
        qty: float,
        reason: str,
    ):
        msg = (
            "🚨 자동매매가 롱 포지션을 잡았어요!\n"
            "📈 조건 충족으로 시장가 매수 체결\n\n"
            f"⚡진입근거 : {reason}\n\n"
            f"🔥종목: {symbol}\n"
            f"⏰시간봉: {interval}\n"
            f"📦수량: {qty}\n"
            f"✅ 진입가 : {entry_price:.6f}\n"
            f"🟢 익절가 : {tp_price:.6f}\n"
            f"🔴 손절가 : {sl_price:.6f}\n"
            "------------------------------------------------"
        )
        self.send_discord(msg)

    def send_trade_close_discord(self, symbol: str, interval: str, price: float, reason: str):
        msg = (
            "🎯 자동매매가 매도를 완료했어요!\n"
            f"사유: {reason}\n\n"
            f"🔥종목: {symbol}\n"
            f"⏰시간봉: {interval}\n"
            f"💵 매도가 : {price:.6f}\n"
            "------------------------------------------------"
        )
        self.send_discord(msg)

    # =========================
    # 전략 로직
    # =========================
    def calc_ma14_from_closed(self, symbol: str, interval: str) -> Optional[float]:
        candles = list(self.closed_candles[symbol][interval])
        if len(candles) < 14:
            return None
        return sum(c.close for c in candles[-14:]) / 14.0

    def calc_prev_ma14(self, symbol: str, interval: str) -> Optional[float]:
        candles = list(self.closed_candles[symbol][interval])
        if len(candles) < 15:
            return None
        return sum(c.close for c in candles[-15:-1]) / 14.0

    def calc_live_ma14(self, symbol: str, interval: str) -> Optional[float]:
        candles = list(self.closed_candles[symbol][interval])
        live = self.live_candle[symbol][interval]
        if len(candles) < 13 or live is None:
            return None
        return (sum(c.close for c in candles[-13:]) + live.close) / 14.0

    def avg_volume_20(self, symbol: str, interval: str) -> Optional[float]:
        candles = list(self.closed_candles[symbol][interval])
        if len(candles) < 20:
            return None
        return sum(c.volume for c in candles[-20:]) / 20.0

    def is_bullish_order_block(self, prev_candle: Candle, curr_candle: Candle) -> bool:
        return (
            prev_candle.open > prev_candle.close
            and curr_candle.close > curr_candle.open
            and curr_candle.open <= prev_candle.close
            and curr_candle.close >= prev_candle.open
        )

    def is_bearish_order_block(self, prev_candle: Candle, curr_candle: Candle) -> bool:
        return (
            prev_candle.close > prev_candle.open
            and curr_candle.open > curr_candle.close
            and curr_candle.open >= prev_candle.close
            and curr_candle.close <= prev_candle.open
        )

    def ma14_cross_on_closed(self, symbol: str, interval: str) -> bool:
        candles = list(self.closed_candles[symbol][interval])
        if len(candles) < 15:
            return False
        prev_candle = candles[-2]
        curr_candle = candles[-1]
        prev_ma = self.calc_prev_ma14(symbol, interval)
        curr_ma = self.calc_ma14_from_closed(symbol, interval)
        if prev_ma is None or curr_ma is None:
            return False
        return prev_ma > prev_candle.high and curr_candle.body_low <= curr_ma <= curr_candle.body_high

    def ma14_cross_live(self, symbol: str, interval: str) -> bool:
        candles = list(self.closed_candles[symbol][interval])
        live = self.live_candle[symbol][interval]
        if len(candles) < 14 or live is None:
            return False
        prev_candle = candles[-1]
        prev_ma = self.calc_ma14_from_closed(symbol, interval)
        current_live_ma = self.calc_live_ma14(symbol, interval)
        if prev_ma is None or current_live_ma is None:
            return False
        return prev_ma > prev_candle.high and live.body_low <= current_live_ma <= live.body_high

    def live_volume_spike(self, symbol: str, interval: str) -> bool:
        live = self.live_candle[symbol][interval]
        avg_vol = self.avg_volume_20(symbol, interval)
        if live is None or avg_vol is None:
            return False
        return live.volume >= avg_vol * VOLUME_SPIKE_MULTIPLIER

    # =========================
    # 거래소 메타 / 초기화
    # =========================
    def bootstrap(self):
        selected_symbols = self.config["selected_symbols"]
        self.closed_candles = {
            symbol: {interval: deque(maxlen=400) for interval in INTERVALS}
            for symbol in selected_symbols
        }
        self.live_candle = {
            symbol: {interval: None for interval in INTERVALS}
            for symbol in selected_symbols
        }
        self.bullish_ob_active = {
            symbol: {interval: False for interval in INTERVALS}
            for symbol in selected_symbols
        }
        self.bullish_ob_time = {
            symbol: {interval: None for interval in INTERVALS}
            for symbol in selected_symbols
        }

        ex_info = self.client.get_exchange_info()
        self.metas = {}
        for s in ex_info["symbols"]:
            if s["symbol"] not in selected_symbols:
                continue

            step_size = 0.0
            tick_size = 0.0
            min_qty = 0.0
            min_notional = 5.0

            for f in s["filters"]:
                if f["filterType"] in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    step_size = float(f.get("stepSize", step_size or 0))
                    min_qty = float(f.get("minQty", min_qty or 0))
                elif f["filterType"] == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", 0))
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", min_notional))

            self.metas[s["symbol"]] = SymbolMeta(
                symbol=s["symbol"],
                quantity_precision=int(s["quantityPrecision"]),
                price_precision=int(s["pricePrecision"]),
                step_size=step_size,
                tick_size=tick_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )

        for symbol in selected_symbols:
            for interval in INTERVALS:
                rows = self.client.get_klines(symbol, interval, CANDLE_LIMIT)
                dq = self.closed_candles[symbol][interval]
                dq.clear()
                for row in rows:
                    dq.append(
                        Candle(
                            open_time=int(row[0]),
                            close_time=int(row[6]),
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5]),
                            is_closed=True,
                        )
                    )
                self.log(f"[{symbol}][{interval}] 과거봉 {len(dq)}개 로드 완료")

    def floor_to_step(self, value: float, step: float, precision: int) -> float:
        if step <= 0:
            return round(value, precision)
        floored = math.floor(value / step) * step
        return float(f"{floored:.{precision}f}")

    def get_wallet_usdt(self) -> float:
        balances = self.client.get_account_balance()
        for row in balances:
            if row["asset"] == "USDT":
                return float(row["availableBalance"])
        return 0.0

    def calc_order_qty(self, symbol: str, market_price: float) -> float:
        meta = self.metas[symbol]
        sizing_mode = self.config["sizing_mode"]
        leverage = self.config["leverage"]

        if sizing_mode == "fixed":
            margin_usdt = self.config["fixed_usdt"]
        else:
            balance = self.get_wallet_usdt()
            margin_usdt = balance * (self.config["balance_ratio"] / 100.0)

        notional = margin_usdt * leverage
        raw_qty = notional / market_price
        qty = self.floor_to_step(raw_qty, meta.step_size, meta.quantity_precision)

        if qty < meta.min_qty:
            raise RuntimeError(f"{symbol} 수량 {qty}가 최소 수량 {meta.min_qty}보다 작습니다.")
        if qty * market_price < meta.min_notional:
            raise RuntimeError(f"{symbol} 주문 가치가 최소 notional {meta.min_notional}보다 작습니다.")

        return qty

    def ensure_futures_modes(self):
        self.log("포지션 모드 설정 시작")
        self.client.set_position_mode_one_way()
        self.log("포지션 모드 설정 완료")

        margin_type = self.config["margin_type"]
        leverage = self.config["leverage"]

        for symbol in self.config["selected_symbols"]:
            self.log(f"[{symbol}] 마진 타입 설정 시작")
            self.client.set_margin_type(symbol, margin_type)
            self.log(f"[{symbol}] 마진 타입 설정 완료")

            self.log(f"[{symbol}] 레버리지 설정 시작")
            self.client.set_leverage(symbol, leverage)
            self.log(f"[{symbol}] 레버리지 설정 완료")

    # =========================
    # 포지션 동기화 / 매매
    # =========================
    def sync_exchange_position_state(self):
        if not self.active_trade["is_active"]:
            return

        now = time.time()
        if now - self.last_sync_ts < SYNC_COOLDOWN_SECONDS:
            return
        self.last_sync_ts = now

        symbol = self.active_trade["symbol"]
        interval = self.active_trade["interval"]

        try:
            positions = self.client.get_position_risk()
        except Exception as e:
            self.log(f"[포지션 동기화 오류] {e}")
            return

        for pos in positions:
            if pos["symbol"] != symbol:
                continue

            amt = float(pos.get("positionAmt", 0))
            entry_price = float(pos.get("entryPrice", 0))
            mark_price = float(pos.get("markPrice", 0))

            if abs(amt) < 1e-12:
                reason = "거래소 TP/SL 또는 수동 청산 감지"
                self.log(f"[동기화] {symbol} 포지션 종료 확인 → active_trade 해제")

                close_price = mark_price if mark_price > 0 else (entry_price if entry_price > 0 else 0.0)
                self.send_trade_close_discord(symbol, interval, close_price, reason)
                self.active_trade = self._empty_trade()
            return

    def enter_long(self, symbol: str, interval: str, entry_price_hint: float, reason: str):
        if self.active_trade["is_active"]:
            self.log(f"[진입 차단] 이미 활성 포지션 존재: {self.active_trade['symbol']} {self.active_trade['interval']}")
            return

        qty = self.calc_order_qty(symbol, entry_price_hint)
        resp = self.client.new_market_order(symbol=symbol, side="BUY", quantity=qty, reduce_only=False)
        avg_price = float(resp.get("avgPrice") or entry_price_hint)

        tp_pct = self.config["tp_percent"] / 100.0 / self.config["leverage"]
        sl_pct = self.config["sl_percent"] / 100.0 / self.config["leverage"]

        tp_price = avg_price * (1 + tp_pct)
        sl_price = avg_price * (1 - sl_pct)

        self.active_trade = {
            "symbol": symbol,
            "interval": interval,
            "entry_price": avg_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "qty": qty,
            "is_active": True,
        }

        self.trade_signal.emit(f"진입: {symbol} {interval} @ {avg_price:.6f}")
        self.log(f"[진입] {symbol} {interval} qty={qty} avg={avg_price:.6f}")

        if self.config["exit_mode"] == "exchange":
            self.client.place_tp_sl_algo(symbol, "SELL", qty, tp_price, sl_price)
            self.log("[거래소 TP/SL 주문 등록 완료]")

        self.send_trade_open_discord(symbol, interval, avg_price, tp_price, sl_price, qty, reason)

    def exit_long(self, symbol: str, interval: str, reason: str):
        if not self.active_trade["is_active"]:
            return

        qty = self.active_trade["qty"]
        resp = self.client.new_market_order(symbol=symbol, side="SELL", quantity=qty, reduce_only=True)
        avg_price = float(resp.get("avgPrice") or 0)

        self.trade_signal.emit(f"청산: {symbol} {interval} @ {avg_price:.6f} ({reason})")
        self.log(f"[청산] {symbol} {interval} qty={qty} avg={avg_price:.6f} reason={reason}")
        self.send_trade_close_discord(symbol, interval, avg_price, reason)

        self.active_trade = self._empty_trade()

    async def monitor_direct_exit(self):
        if self.config["exit_mode"] != "bot":
            return
        if not self.active_trade["is_active"]:
            return

        symbol = self.active_trade["symbol"]
        interval = self.active_trade["interval"]
        live = self.live_candle.get(symbol, {}).get(interval)

        if live is None:
            return

        if live.close >= self.active_trade["tp_price"]:
            self.exit_long(symbol, interval, "TP 도달")
        elif live.close <= self.active_trade["sl_price"]:
            self.exit_long(symbol, interval, "SL 도달")

    # =========================
    # 캔들 처리
    # =========================
    async def handle_closed_candle(self, symbol: str, interval: str):
        if self.config["exit_mode"] == "exchange":
            self.sync_exchange_position_state()

        candles = list(self.closed_candles[symbol][interval])
        if len(candles) < 2:
            return

        prev_candle = candles[-2]
        curr_candle = candles[-1]

        bullish_ob = self.is_bullish_order_block(prev_candle, curr_candle)
        bearish_ob = self.is_bearish_order_block(prev_candle, curr_candle)
        cross_closed = self.ma14_cross_on_closed(symbol, interval)

        if bullish_ob:
            self.bullish_ob_active[symbol][interval] = True
            self.bullish_ob_time[symbol][interval] = curr_candle.open_time
            self.log(f"[{symbol}][{interval}] 상승형 오더블록 감지")

        if bearish_ob:
            self.log(f"[{symbol}][{interval}] 하락형 오더블록 감지")

            if (
                self.active_trade["is_active"]
                and self.active_trade["symbol"] == symbol
                and self.active_trade["interval"] == interval
            ):
                current_price = curr_candle.close
                entry_price = self.active_trade["entry_price"]
                pnl_percent = ((current_price - entry_price) / entry_price) * 100 * self.config["leverage"]

                if pnl_percent < self.config["tp_percent"]:
                    if self.config["exit_mode"] == "bot":
                        self.exit_long(symbol, interval, "하락형 오더블록 익절")
                    else:
                        self.log("[참고] exchange 모드에서는 하락형 오더블록 즉시청산 대신 거래소 TP/SL 우선")

            self.bullish_ob_active[symbol][interval] = False
            self.bullish_ob_time[symbol][interval] = None

        if self.bullish_ob_active[symbol][interval] and not self.active_trade["is_active"] and cross_closed:
            reason = "직전 상승형 오더블록 + MA14 몸통 통과"
            self.enter_long(symbol, interval, curr_candle.close, reason)
            self.bullish_ob_active[symbol][interval] = False
            self.bullish_ob_time[symbol][interval] = None

    async def handle_live_candle(self, symbol: str, interval: str):
        if self.config["exit_mode"] == "exchange":
            self.sync_exchange_position_state()

        live = self.live_candle[symbol][interval]
        if live is None:
            return

        if self.active_trade["is_active"]:
            await self.monitor_direct_exit()
            return

        if not self.bullish_ob_active[symbol][interval]:
            return
        if not self.ma14_cross_live(symbol, interval):
            return
        if not self.live_volume_spike(symbol, interval):
            return

        reason = "직전 상승형 오더블록 + 실시간 MA14 몸통 통과 + 거래량 급증"
        self.enter_long(symbol, interval, live.close, reason)
        self.bullish_ob_active[symbol][interval] = False
        self.bullish_ob_time[symbol][interval] = None

    def parse_ws_message(self, payload: dict) -> Candle:
        k = payload["data"]["k"]
        return Candle(
            open_time=int(k["t"]),
            close_time=int(k["T"]),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            is_closed=bool(k["x"]),
        )

    async def ws_loop(self):
        streams = "/".join(
            [f"{symbol.lower()}@kline_{interval}" for symbol in self.config["selected_symbols"] for interval in INTERVALS]
        )
        ws_base = "wss://fstream.binancefuture.com/market" if self.config["testnet"] else WS_BASE_URL
        url = f"{ws_base}/stream?streams={streams}"

        while self.running:
            try:
                self.log(f"WS 연결: {url}")
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.status("실행 중")

                    while self.running:
                        raw = await ws.recv()
                        payload = json.loads(raw)
                        symbol = payload["data"]["s"]
                        interval = payload["data"]["k"]["i"]
                        candle = self.parse_ws_message(payload)

                        if candle.is_closed:
                            self.closed_candles[symbol][interval].append(candle)
                            self.live_candle[symbol][interval] = None
                            await self.handle_closed_candle(symbol, interval)
                        else:
                            self.live_candle[symbol][interval] = candle
                            await self.handle_live_candle(symbol, interval)

            except Exception as e:
                self.log(f"WS 오류: {e}")
                await asyncio.sleep(3)

    async def async_main(self):
        self.client = BinanceFuturesClient(
            api_key=self.config["api_key"],
            api_secret=self.config["api_secret"],
            testnet=self.config["testnet"],
        )

        self.ensure_futures_modes()
        self.bootstrap()
        self.send_program_started_discord()
        await self.ws_loop()

    def run(self):
        self.running = True
        self.status("초기화 중")
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.async_main())
        except Exception as e:
            self.log(f"실행 오류: {e}")
            self.status("오류 발생")
        finally:
            if self.client:
                self.client.close()
            if self.loop:
                self.loop.close()
            self.status("중지됨")


class LoginWindow(QDialog):
    login_success = Signal(dict)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("로그인")
        self.setWindowIcon(QIcon(APP_ICON_PATH))
        self.setFixedSize(420, 220)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("꼬라니 바이낸스 자동매매 로그인")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        form = QFormLayout()
        self.id_edit = QLineEdit()
        self.code_edit = QLineEdit()
        self.code_edit.setEchoMode(QLineEdit.Password)

        form.addRow("유저 ID", self.id_edit)
        form.addRow("인증 코드", self.code_edit)
        layout.addLayout(form)

        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)

        btn_row = QHBoxLayout()
        self.login_btn = QPushButton("로그인")
        self.close_btn = QPushButton("닫기")
        btn_row.addWidget(self.login_btn)
        btn_row.addWidget(self.close_btn)
        layout.addLayout(btn_row)

        self.login_btn.clicked.connect(self.try_login)
        self.close_btn.clicked.connect(self.reject)

    def try_login(self):
        user_id = self.id_edit.text().strip()
        auth_code = self.code_edit.text().strip()

        if not user_id or not auth_code:
            self.status_label.setText("유저 ID와 인증 코드를 입력해주세요.")
            return

        try:
            profile = fetch_user_profile_from_github(user_id)
        except Exception as e:
            QMessageBox.critical(self, "로그인 실패", str(e))
            return

        if auth_code != profile["auth_code"]:
            self.status_label.setText("인증 코드가 올바르지 않습니다. 관리자에게 문의하세요.")
            return

        self.login_success.emit(profile)
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self, login_profile: Optional[dict] = None):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(QIcon(APP_ICON_PATH))
        self.resize(1180, 860)
        self.worker: Optional[TradingWorker] = None
        self.symbol_checks: Dict[str, QCheckBox] = {}
        self.login_profile = login_profile
        self._build_ui()
        self._apply_login_profile()

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)

        api_group = QGroupBox("API / Discord 설정")
        api_form = QFormLayout(api_group)
        self.api_key_edit = QLineEdit()
        self.api_secret_edit = QLineEdit()
        self.api_secret_edit.setEchoMode(QLineEdit.Password)
        self.discord_webhook_edit = QLineEdit()
        self.testnet_check = QCheckBox("테스트넷 사용")
        api_form.addRow("Binance API Key", self.api_key_edit)
        api_form.addRow("Binance API Secret", self.api_secret_edit)
        api_form.addRow("Discord Webhook", self.discord_webhook_edit)
        api_form.addRow("환경", self.testnet_check)
        main.addWidget(api_group)

        symbol_group = QGroupBox("1. 종목 선택")
        symbol_layout = QGridLayout(symbol_group)
        for idx, symbol in enumerate(SYMBOLS):
            cb = QCheckBox(symbol)
            cb.setChecked(symbol in ["BTCUSDT", "ETHUSDT"])
            self.symbol_checks[symbol] = cb
            symbol_layout.addWidget(cb, idx // 3, idx % 3)
        main.addWidget(symbol_group)

        trade_group = QGroupBox("2. 주문 / 청산 설정")
        trade_layout = QGridLayout(trade_group)

        self.leverage_spin = QSpinBox()
        self.leverage_spin.setRange(1, 125)
        self.leverage_spin.setValue(10)

        self.margin_combo = QComboBox()
        self.margin_combo.addItems(["CROSSED", "ISOLATED"])

        self.exit_mode_bot = QRadioButton("자동 시장가 청산")
        self.exit_mode_exchange = QRadioButton("TP/SL 자동 등록")
        self.exit_mode_bot.setChecked(True)

        self.sizing_fixed = QRadioButton("USDT(직접입력)")
        self.sizing_ratio = QRadioButton("계좌 잔고 비율(%)")
        self.sizing_fixed.setChecked(True)

        self.exit_mode_group = QButtonGroup(self)
        self.exit_mode_group.addButton(self.exit_mode_bot)
        self.exit_mode_group.addButton(self.exit_mode_exchange)

        self.sizing_mode_group = QButtonGroup(self)
        self.sizing_mode_group.addButton(self.sizing_fixed)
        self.sizing_mode_group.addButton(self.sizing_ratio)

        self.fixed_usdt_spin = QDoubleSpinBox()
        self.fixed_usdt_spin.setRange(5, 100000)
        self.fixed_usdt_spin.setDecimals(2)
        self.fixed_usdt_spin.setValue(50)

        self.balance_ratio_spin = QDoubleSpinBox()
        self.balance_ratio_spin.setRange(0.1, 100.0)
        self.balance_ratio_spin.setDecimals(2)
        self.balance_ratio_spin.setValue(10.0)
        self.balance_ratio_spin.setSuffix(" %")

        self.tp_spin = QDoubleSpinBox()
        self.tp_spin.setRange(0.1, 100.0)
        self.tp_spin.setDecimals(2)
        self.tp_spin.setValue(5.0)
        self.tp_spin.setSuffix(" %")

        self.sl_spin = QDoubleSpinBox()
        self.sl_spin.setRange(0.1, 100.0)
        self.sl_spin.setDecimals(2)
        self.sl_spin.setValue(5.0)
        self.sl_spin.setSuffix(" %")

        trade_layout.addWidget(QLabel("레버리지"), 0, 0)
        trade_layout.addWidget(self.leverage_spin, 0, 1)
        trade_layout.addWidget(QLabel("마진 방식"), 0, 2)
        trade_layout.addWidget(self.margin_combo, 0, 3)

        trade_layout.addWidget(QLabel("청산 방식"), 1, 0)
        trade_layout.addWidget(self.exit_mode_bot, 1, 1, 1, 2)
        trade_layout.addWidget(self.exit_mode_exchange, 1, 3, 1, 2)

        trade_layout.addWidget(QLabel("주문 크기 방식"), 2, 0)
        trade_layout.addWidget(self.sizing_fixed, 2, 1)
        trade_layout.addWidget(self.fixed_usdt_spin, 2, 2)
        trade_layout.addWidget(self.sizing_ratio, 2, 3)
        trade_layout.addWidget(self.balance_ratio_spin, 2, 4)

        trade_layout.addWidget(QLabel("익절(사용자 입력)"), 3, 0)
        trade_layout.addWidget(self.tp_spin, 3, 1)
        trade_layout.addWidget(QLabel("손절(사용자 입력)"), 3, 2)
        trade_layout.addWidget(self.sl_spin, 3, 3)

        main.addWidget(trade_group)

        info_group = QGroupBox("3. 📌공지📌")
        info_layout = QVBoxLayout(info_group)
        info_layout.addWidget(QLabel("바이낸스 API키와 Secret키는 로그인 후 자동 입력됩니다."))
        info_layout.addWidget(QLabel("디스코드 웹훅은 로그인 후 자동 입력됩니다."))
        info_layout.addWidget(QLabel("프로그램은 24시간 감시하며 조건 충족 시 자동 매수/매도합니다."))
        info_layout.addWidget(QLabel("포지션은 1개만 진입합니다.(추후 Update 예정)"))
        info_layout.addWidget(QLabel("종목은 최소 1개 이상을 선택해야 합니다."))
        info_layout.addWidget(QLabel("사용기간을 연장하시려면 관리자에게 문의하세요. (관리자1 : WJ / 관리자2 : JG)"))
        main.addWidget(info_group)

        row = QHBoxLayout()
        self.start_btn = QPushButton("자동매매 시작")
        self.stop_btn = QPushButton("자동매매 중지")
        self.stop_btn.setEnabled(False)
        self.status_label = QLabel("대기 중")
        self.status_label.setAlignment(Qt.AlignCenter)
        row.addWidget(self.start_btn)
        row.addWidget(self.stop_btn)
        row.addWidget(self.status_label, 1)
        main.addLayout(row)

        self.trade_view = QPlainTextEdit()
        self.trade_view.setReadOnly(True)
        self.trade_view.setPlaceholderText("체결/포지션 로그")

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("시스템 로그")

        main.addWidget(QLabel("체결 로그"))
        main.addWidget(self.trade_view, 1)
        main.addWidget(QLabel("시스템 로그"))
        main.addWidget(self.log_view, 2)

        self.start_btn.clicked.connect(self.start_bot)
        self.stop_btn.clicked.connect(self.stop_bot)
        self.sizing_fixed.toggled.connect(self._refresh_size_mode)
        self.sizing_ratio.toggled.connect(self._refresh_size_mode)
        self._refresh_size_mode()

    def _apply_login_profile(self):
        if not self.login_profile:
            return

        self.api_key_edit.setText(self.login_profile.get("api_key", ""))
        self.api_secret_edit.setText(self.login_profile.get("api_secret", ""))
        self.discord_webhook_edit.setText(self.login_profile.get("discord_webhook", ""))

        self.api_key_edit.setReadOnly(True)
        self.api_secret_edit.setReadOnly(True)
        self.discord_webhook_edit.setReadOnly(True)

    def _refresh_size_mode(self):
        self.fixed_usdt_spin.setEnabled(self.sizing_fixed.isChecked())
        self.balance_ratio_spin.setEnabled(self.sizing_ratio.isChecked())

    def selected_symbols(self) -> List[str]:
        return [symbol for symbol, cb in self.symbol_checks.items() if cb.isChecked()]

    def build_config(self) -> dict:
        symbols = self.selected_symbols()
        if not symbols:
            raise ValueError("최소 1개 종목은 선택해야 합니다.")
        if not self.api_key_edit.text().strip() or not self.api_secret_edit.text().strip():
            raise ValueError("API Key / Secret이 비어 있습니다.")

        exit_mode = "bot" if self.exit_mode_bot.isChecked() else "exchange"
        sizing_mode = "fixed" if self.sizing_fixed.isChecked() else "ratio"

        return {
            "api_key": self.api_key_edit.text().strip(),
            "api_secret": self.api_secret_edit.text().strip(),
            "discord_webhook": self.discord_webhook_edit.text().strip(),
            "testnet": self.testnet_check.isChecked(),
            "selected_symbols": symbols,
            "leverage": int(self.leverage_spin.value()),
            "margin_type": self.margin_combo.currentText(),
            "exit_mode": exit_mode,
            "sizing_mode": sizing_mode,
            "fixed_usdt": float(self.fixed_usdt_spin.value()),
            "balance_ratio": float(self.balance_ratio_spin.value()),
            "tp_percent": float(self.tp_spin.value()),
            "sl_percent": float(self.sl_spin.value()),
        }

    def start_bot(self):
        try:
            config = self.build_config()
        except Exception as e:
            QMessageBox.warning(self, "입력 오류", str(e))
            return

        self.worker = TradingWorker(config)
        self.worker.log_signal.connect(self.append_log)
        self.worker.status_signal.connect(self.set_status)
        self.worker.trade_signal.connect(self.append_trade)
        self.worker.start()

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.set_status("시작 중")
        self.append_log(f"선택 종목: {', '.join(config['selected_symbols'])}")

    def stop_bot(self):
        if self.worker:
            self.worker.stop()
            self.worker.wait(5000)
            self.worker = None

        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.set_status("중지됨")

    def append_log(self, text: str):
        self.log_view.appendPlainText(text)

    def append_trade(self, text: str):
        self.trade_view.appendPlainText(text)

    def set_status(self, text: str):
        self.status_label.setText(text)

    def closeEvent(self, event):
        self.stop_bot()
        super().closeEvent(event)


def main():
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("corani.coin.auto.system")

    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(APP_ICON_PATH))

    login_window = LoginWindow()
    main_window_holder = {"window": None}

    def open_main(profile: dict):
        main_window = MainWindow(login_profile=profile)
        main_window.show()
        main_window_holder["window"] = main_window

    login_window.login_success.connect(open_main)

    if login_window.exec() == QDialog.Accepted:
        sys.exit(app.exec())
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()