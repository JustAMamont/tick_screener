# cython: language_level=3

from cython import bint
import asyncio
import uvloop
from dataclasses import dataclass, field
import utils.cythonic_utils as cu
cimport utils.cythonic_utils as cu
import ccxt.pro as ccxt
from aiogram import Bot
import json
import os
import aiofiles
import time
import gc 
from lumina import Lumina
from typing import Optional, List, Set, Dict, Any
from aiohttp.client_exceptions import ClientConnectionError
import numpy as np 
cimport numpy as np

np.import_array()
uvloop.install()

# capture_caller=False, так как мы передаем location вручную для скорости и точности
lum = Lumina.get_logger(capture_caller=False)

@dataclass
class DataCache:
    config: Optional[Dict[str, Any]] = None
    historical_pairlist: Set[str] = field(default_factory=set)
    pairlist: Set[str] = field(default_factory=set)
    alert_tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    alerts: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=200))

    @staticmethod
    def clear_cache():
        DataCache.config = None
        DataCache.historical_pairlist = set()
        DataCache.pairlist = set()
        DataCache.alert_tasks = {}
        DataCache.alerts = asyncio.Queue(maxsize=200)

cache_instance = DataCache()
DataCache.config = cache_instance.config
DataCache.historical_pairlist = cache_instance.historical_pairlist
DataCache.pairlist = cache_instance.pairlist
DataCache.alert_tasks = cache_instance.alert_tasks
DataCache.alerts = cache_instance.alerts


class ConfigManager:
    @staticmethod
    async def read_config() -> dict:
        async with aiofiles.open('config.json', 'r') as file:
            content = await file.read()
            return json.loads(content)


class SymbolFormatter:
    @staticmethod
    def format(str symbol, str curr_type, str delim =''):
        cdef str delim_symbol
        delim_symbol = symbol.replace('/', delim)
        if curr_type == 'spot':
            return delim_symbol
        else:
            return delim_symbol.split(':')[0]


class TelegramSender:
    def __init__(self, bot: Bot):
        self.bot = bot

    async def run(self):
        cdef tuple alert, prev_alert = ()
        while True:
            try:
                alert = await DataCache.alerts.get()
                DataCache.alerts.task_done()
                if alert and alert != prev_alert:
                    # PROD: Отправка в Telegram
                    await self.bot.send_message(DataCache.config['telegram']['chat_id'], text=alert[2], parse_mode="MARKDOWN")
                    
                    # Логируем факт отправки
                    lum.debug("Alert sent", text=alert[2], location=("scanner.pyx", 86))
                    prev_alert = alert
            except (asyncio.exceptions.CancelledError, KeyboardInterrupt):
                break
            except Exception as e:
                lum.error(f"TelegramSender.run: ", exc=e, location=("scanner.pyx", 91))


class DataProcessor:
    def __init__(self, exc_id: str, bot: Bot, symbols: List[str], use_batching: bint):
        self.exc_id = exc_id
        self.exc = None
        self.bot = bot
        self.symbols = symbols
        self.use_batching = use_batching
        self.task = None
        self.prev_ts_map = {symbol: 0 for symbol in symbols}
        self.trades_cache: Dict[str, cu.TimeWindowTrades] = {}
        self.is_running = True
        self.symbols_str = ", ".join(self.symbols[:2]) + ('...' if len(self.symbols) > 2 else '')
        self.current_trange: Optional[int] = None
        self.reconnect_delay = 1.0
        self.max_reconnect_delay = 30.0

    def _create_exchange_instance(self):
        return getattr(ccxt, self.exc_id)()

    async def _initialize_connection(self):
        await self._close_connection()
        
        with lum.profile("DP._initialize_connection", location=("scanner.pyx", 116)):
            lum.info(f"DataProcessor: Инициализация соединения и кэша...", chunk=self.symbols_str, location=("scanner.pyx", 117))
            self.exc = self._create_exchange_instance()
            timeframe_seconds = DataCache.config.get('trange', 1)
            self.current_trange = timeframe_seconds

            window_size_seconds = int(timeframe_seconds * 2)
            if window_size_seconds <= timeframe_seconds:
                window_size_seconds = timeframe_seconds + 1

            self.trades_cache = {
                s: cu.TimeWindowTrades(window_seconds=window_size_seconds, maxlen=2000)
                for s in self.symbols
            }
            lum.info(f"DataProcessor: Кэш инициализирован.", 
                chunk=self.symbols_str, tf_sec=self.current_trange, window_size_sec=window_size_seconds, location=("scanner.pyx", 131))
            self.reconnect_delay = 1.0


    async def _close_connection(self):
        if self.exc:
            lum.warning(f"DataProcessor: Закрытие соединения...", chunk=self.symbols_str, location=("scanner.pyx", 137))
            try:
                await self.exc.close()
            except Exception as e:
                lum.error(f"DataProcessor: Ошибка при закрытии соединения: {e}", exc=e, chunk=self.symbols_str, location=("scanner.pyx", 141))
            finally:
                self.exc = None

    async def process(self):
        self.task = asyncio.current_task()
        try:
            while self.is_running:
                    await self._wait_for_config()
                    try:
                        await self._initialize_connection()
                        if self.use_batching:
                            await self._process_batch()
                        else:
                            await self._process_single()
                    except (ccxt.NetworkError, ccxt.DDoSProtection, ClientConnectionError, asyncio.TimeoutError) as e:
                        lum.warning(f"Ошибка сети: Перезапуск через {self.reconnect_delay:.1f}с.",
                            chunk=self.symbols_str, network_exc=type(e).__name__, location=("scanner.pyx", 158))
                        await self._close_connection()
                        await asyncio.sleep(self.reconnect_delay)
                        self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)
                    except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
                        lum.info(f"Задача отменена.", chunk=self.symbols_str, location=("scanner.pyx", 163))
                        self.is_running = False
                    except Exception as e:
                        lum.critical(f"КРИТИЧЕСКАЯ ОШИБКА в цикле!", exc=e, chunk=self.symbols_str, location=("scanner.pyx", 166))
                        await asyncio.sleep(5)
        finally:
            await self.close("Finished processing")

    async def _check_and_reinitialize(self):
        if DataCache.config is None or self.current_trange is None:
            return False
        if DataCache.config.get('trange') != self.current_trange:
            lum.warning(
                f"DataProcessor: Смена таймфрейма. Пересоздание...",
                chunk=self.symbols_str, location=("scanner.pyx", 177)
            )
            return True
        return False

    async def _process_batch(self):
        while self.is_running:
            if await self._check_and_reinitialize(): break 
            new_trades = await self.exc.watch_trades_for_symbols(symbols=self.symbols)
            await self._handle_new_trades(new_trades)

    async def _process_single(self):
        tasks = [self._watch_one_symbol(s) for s in self.symbols if s in DataCache.pairlist]
        if not tasks:
            await asyncio.sleep(10)
            return
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch_one_symbol(self, symbol: str):
        while self.is_running:
            try:
                new_trades = await self.exc.watch_trades(symbol=symbol)
                await self._handle_new_trades(new_trades)
            except Exception:
                break
    
    async def _handle_new_trades(self, new_trades: List[Dict]):
        cdef cu.TimeWindowTrades trade_window
        cdef set symbols_to_process = set()
        cdef dict trade
        cdef str symbol, message
        cdef long long ts, prev_ts
        
        # Профилирование обработки трейдов
        with lum.profile("DP._handle_new_trades", min_duration_ms=10, location=("scanner.pyx", 211)):
            for trade in new_trades:
                symbol = trade.get('symbol')
                if symbol and symbol in self.trades_cache:
                    trade_window = self.trades_cache[symbol]
                    trade_window.append(trade['timestamp'], trade['price'], trade['cost'])
                    symbols_to_process.add(symbol)

            if not symbols_to_process:
                return

            curr_type = DataCache.config.get('currency_type', 'spot')
            delim = DataCache.config.get('delimiter', '')
            tf_str = f"{DataCache.config['trange']}s".encode('utf-8')

            for symbol in symbols_to_process:
                trade_window = self.trades_cache[symbol]

                message, ts = trade_window.check_alert(
                    tf_str,
                    DataCache.config,
                    SymbolFormatter.format(symbol, curr_type, delim)
                )

                prev_ts = self.prev_ts_map.get(symbol, 0)
                if message and ts != prev_ts:
                    try:
                        DataCache.alerts.put_nowait((symbol, ts, message))
                    except asyncio.QueueFull:
                        lum.warning("Alert queue full", symbol=symbol, location=("scanner.pyx", 240))
                    self.prev_ts_map[symbol] = ts

    async def _wait_for_config(self):
        while not DataCache.config:
            await asyncio.sleep(0.1)

    async def close(self, source: str):
        self.is_running = False
        lum.info(f"DataProcessor.close", source=source, chunk=self.symbols_str, location=("scanner.pyx", 249))
        await self._close_connection()


class ProcessesLoop:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.background_tasks = []

    async def run(self, str exc_id, int multi_symbol_chunk_size, 
                  int single_symbol_chunk_size, float task_launch_delay):
        cdef list pairlist_to_process, chunked_list, chunk
        cdef bint supports_batching

        try:
            temp_exc = getattr(ccxt, exc_id)()
            supports_batching = temp_exc.has.get('watchTradesForSymbols', False)
            await temp_exc.close()
        except Exception as e:
            lum.error("Ошибка проверки возможностей биржи", exc=e, location=("scanner.pyx", 268))
            return

        try:
            while not DataCache.pairlist:
                lum.info("ProcessesLoop: Ожидание пар...", location=("scanner.pyx", 273))
                await asyncio.sleep(5)

            pairlist_to_process = list(DataCache.pairlist)

            # PROD: Ограничение убрано
            # if len(pairlist_to_process) > 10:
            #     lum.warning(f"DEBUG LIMIT...", location=("scanner.pyx", 270))
            #     pairlist_to_process = pairlist_to_process[:10]
            
            use_batching_mode = supports_batching
            chunk_size = multi_symbol_chunk_size if use_batching_mode else single_symbol_chunk_size
            
            chunked_list = [
                pairlist_to_process[i:i + chunk_size]
                for i in range(0, len(pairlist_to_process), chunk_size)
            ]

            lum.info(f"ProcessesLoop: Старт. Пар: {len(pairlist_to_process)}, Чанков: {len(chunked_list)}", location=("scanner.pyx", 291))

            DataCache.alert_tasks.clear()

            for chunk in chunked_list:
                if not chunk: continue
                
                processor = DataProcessor(exc_id, self.bot, chunk, use_batching=use_batching_mode)
                task = asyncio.create_task(processor.process())
                
                self.background_tasks.append(task)
                for symbol in chunk:
                    DataCache.alert_tasks[symbol] = task
                
                await asyncio.sleep(task_launch_delay)

            lum.success("Воркеры запущены.", location=("scanner.pyx", 305))
            if self.background_tasks:
                await asyncio.gather(*self.background_tasks)

        except asyncio.CancelledError:
            lum.warning("ProcessesLoop: Cancelled", location=("scanner.pyx", 310))
        except Exception as e:
            lum.critical(f"ProcessesLoop error", exc=e, location=("scanner.pyx", 312))
        finally:
            await self.cleanup()

    async def cleanup(self):
        lum.info("ProcessesLoop: Cleanup...", location=("scanner.pyx", 317))
        for task in self.background_tasks:
            if task and not task.done():
                task.cancel()

        DataCache.alert_tasks.clear()
        self.background_tasks.clear()


class PairlistLoop:
    def __init__(self, bot: Bot):
        self.bot = bot

    async def run(self, str exc_id):
        loop = asyncio.get_running_loop()
        while True:
            try:
                if DataCache.config:
                    # Запуск в потоке, чтобы не блочить event loop
                    await loop.run_in_executor(None, self._sync_load_markets, exc_id)
                
                await asyncio.sleep(120)
            except asyncio.CancelledError:
                break
            except Exception as e:
                lum.error(f"PairlistLoop error", exc=e, location=("scanner.pyx", 339))
                await asyncio.sleep(10)

    def _sync_load_markets(self, exc_id):
        # Профиль для тяжелой задачи
        with lum.profile("Pairlist Updater", location=("scanner.pyx", 344)):
            import ccxt as ccxt_sync 
            exc = None
            try:
                exc = getattr(ccxt_sync, exc_id)()
                curr_type = DataCache.config.get('currency_type', 'spot')
                delim = DataCache.config.get('delimiter', '') # Добавил получение разделителя
                
                # Блокирующий вызов
                markets = exc.load_markets(reload=True)
                
                active_symbols = set(
                    s for s, m in markets.items() 
                    if m.get('active') 
                    and m.get('type') == curr_type 
                    and m.get('quote') in ('USDT', 'USDC')
                    and s not in DataCache.config.get('blacklist', [])
                )

                DataCache.pairlist.intersection_update(active_symbols)
                DataCache.pairlist.update(active_symbols)

                if len(DataCache.historical_pairlist) == 0:
                    DataCache.historical_pairlist.update(active_symbols)
                else:
                    new_ones = active_symbols - DataCache.historical_pairlist
                    if new_ones:
                        for symbol in new_ones:
                            if symbol not in DataCache.historical_pairlist:
                                # Формируем сообщение о листинге
                                msg_text = f"⚡️ ⚡️ ⚡️ LISTING!!! `{SymbolFormatter.format(symbol, curr_type, delim)}` ⚡️ ⚡️ ⚡️"
                                # Отправляем в очередь (ts=0, чтобы точно прошло)
                                try:
                                    DataCache.alerts.put_nowait((symbol, 0, msg_text))
                                    lum.success(f"Listing detected: {symbol}", location=("scanner.pyx", 378))
                                except asyncio.QueueFull:
                                    lum.warning(f"Alert queue full (Listing): {symbol}", location=("scanner.pyx", 380))
                                
                                DataCache.historical_pairlist.add(symbol)

                lum.success(f"Markets loaded. Active: {len(active_symbols)}", location=("scanner.pyx", 384))

            except Exception as e:
                lum.error("Error loading markets (Thread)", exc=e, location=("scanner.pyx", 387))
            finally:
                if exc: del exc; del ccxt_sync
                gc.collect()


class ConfigUpdater:
    def __init__(self): pass
    async def update(self):
        last_modified = 0.0
        while True:
            try:
                if os.path.exists('config.json'):
                    mtime = os.path.getmtime('config.json')
                    if mtime > last_modified:
                        DataCache.config = await ConfigManager.read_config()
                        last_modified = mtime
                        lum.success("Config updated", location=("scanner.pyx", 404))
                await asyncio.sleep(5)
            except asyncio.CancelledError: break
            except Exception as e:
                lum.error("ConfigUpdater error", exc=e, location=("scanner.pyx", 408))
                await asyncio.sleep(5)


class Main:
    def __init__(self, exc_id: str, bot: Bot,
                 multi_symbol_chunk_size: int = 40,
                 single_symbol_chunk_size: int = 100,
                 task_launch_delay: float = 1.0):
        self.exc_id = exc_id
        self.bot = bot
        self.tasks = []
        self.multi_symbol_chunk_size = multi_symbol_chunk_size
        self.single_symbol_chunk_size = single_symbol_chunk_size
        self.task_launch_delay = task_launch_delay

    async def run_tasks(self):
        lum.info(f"Запуск сканера...", location=("scanner.pyx", 425))
        try:
            DataCache.config = await ConfigManager.read_config()
            
            op_task = asyncio.create_task(ConfigUpdater().update())
            pl_task = asyncio.create_task(PairlistLoop(self.bot).run(self.exc_id))
            tg_task = asyncio.create_task(TelegramSender(self.bot).run())
            data_task = asyncio.create_task(
                ProcessesLoop(self.bot).run(
                    exc_id=self.exc_id,
                    multi_symbol_chunk_size=self.multi_symbol_chunk_size,
                    single_symbol_chunk_size=self.single_symbol_chunk_size,
                    task_launch_delay=self.task_launch_delay
                )
            )
            self.tasks = [op_task, pl_task, tg_task, data_task]
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            lum.critical(f"Main crash", exc=e, location=("scanner.pyx", 445))
        finally:
            lum.info("Shutting down...", location=("scanner.pyx", 447))
            for task in self.tasks:
                if not task.done(): task.cancel()
            lum.shutdown()