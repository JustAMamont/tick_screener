# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False

from libc.math cimport floor, fabs, ceil
from libc.string cimport strlen, memcpy
from libc.stdlib cimport atoi, malloc, free
import numpy as np
cimport numpy as np

np.import_array()

# --- Константы индексов ---
cdef int IDX_TS    = 0
cdef int IDX_PRICE = 1
cdef int IDX_COST  = 2

# C-структура для свечи
cdef struct Candle:
    long long ts
    double open
    double high
    double low
    double close
    double volume

cdef class TimeWindowTrades:
    def __cinit__(self, int window_seconds, int maxlen=2000):
        self.window_ms = <long long>window_seconds * 1000
        self.maxlen = maxlen
        self.data = np.empty((maxlen, 3), dtype=np.double)
        self.head = 0
        self.tail = 0
        self.size = 0

    cpdef void append(self, double ts, double price, double cost):
        cdef long long current_ts = <long long>ts
        
        while self.size > 0:
            if current_ts - <long long>self.data[self.head, IDX_TS] > self.window_ms:
                self.head = (self.head + 1) % self.maxlen
                self.size -= 1
            else:
                break
        
        self.data[self.tail, IDX_TS] = ts
        self.data[self.tail, IDX_PRICE] = price
        self.data[self.tail, IDX_COST] = cost
        
        self.tail = (self.tail + 1) % self.maxlen
        
        if self.size < self.maxlen:
            self.size += 1
        else:
            self.head = (self.head + 1) % self.maxlen

    cdef int _parse_timeframe(self, char* timeframe):
        cdef int amount
        cdef char unit = timeframe[strlen(timeframe) - 1]
        cdef char[32] amount_str
        cdef size_t length = strlen(timeframe) - 1
        if length > 31: length = 31
        
        memcpy(amount_str, timeframe, length)
        amount_str[length] = 0
        amount = atoi(amount_str)
        
        cdef int scale = 1
        if unit == b'm': scale = 60
        elif unit == b'h': scale = 3600
        elif unit == b's': scale = 1
        
        return amount * scale

    cpdef tuple check_alert(self, char* timeframe, dict config, str symbol):
        if self.size < 2:
            return "", 0

        cdef long long tf_ms = self._parse_timeframe(timeframe) * 1000
        cdef int idx
        cdef int count = 0
        
        cdef Candle current_candle
        current_candle.ts = 0
        
        cdef Candle last_closed_candle
        last_closed_candle.ts = 0
        
        cdef long long trade_ts, open_time
        cdef double price, cost
        
        idx = self.head
        while count < self.size:
            trade_ts = <long long>self.data[idx, IDX_TS]
            price = self.data[idx, IDX_PRICE]
            cost = self.data[idx, IDX_COST]
            
            open_time = (trade_ts // tf_ms) * tf_ms
            
            if current_candle.ts == 0:
                current_candle.ts = open_time
                current_candle.open = price
                current_candle.high = price
                current_candle.low = price
                current_candle.close = price
                current_candle.volume = cost
            elif open_time == current_candle.ts:
                if price > current_candle.high: current_candle.high = price
                if price < current_candle.low: current_candle.low = price
                current_candle.close = price
                current_candle.volume += cost
            else:
                last_closed_candle = current_candle
                
                current_candle.ts = open_time
                current_candle.open = price
                current_candle.high = price
                current_candle.low = price
                current_candle.close = price
                current_candle.volume = cost
            
            idx = (idx + 1) % self.maxlen
            count += 1
            
        if last_closed_candle.ts == 0:
            return "", 0

        cdef double price_change_pct = 0.0
        
        if last_closed_candle.close > last_closed_candle.open:
            # Для зеленой свечи: (High - Low) / Low
            if last_closed_candle.low != 0:
                price_change_pct = (last_closed_candle.high - last_closed_candle.low) / last_closed_candle.low * 100.0
        elif last_closed_candle.close < last_closed_candle.open:
            # Для красной свечи: (Low - High) / High (будет отрицательным)
            if last_closed_candle.high != 0:
                price_change_pct = (last_closed_candle.low - last_closed_candle.high) / last_closed_candle.high * 100.0
        else:
            price_change_pct = 0.0

        cdef double return_limit = config.get('return_limit', 0.5)
        cdef double volume_limit = config.get('volume_limit', 0.0)

        if (fabs(price_change_pct) >= return_limit) and (last_closed_candle.volume >= volume_limit):
            return self._format_message(config, symbol, price_change_pct, last_closed_candle.volume), <long long>last_closed_candle.ts
            
        return "", 0

    cdef str _format_message(self, dict config, str symbol, double price_return, double total_cost):
        cdef str green = "\U0001F7E2"
        cdef str red = "\U0001F534"
        
        # Логика эмодзи: каждые 10% добавляют кружок
        cdef double abs_return = fabs(price_return)
        cdef int tens = <int>(abs_return / 10.0) + 1
        
        cdef str emo = ""
        if price_return > 0:
            emo = green * tens
        else:
            emo = red * tens
            
        return f"`{symbol}` {emo} {price_return:.3f}%\nVol: {total_cost:.2f}$"