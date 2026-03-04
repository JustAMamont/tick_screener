# cython: language_level=3

cimport numpy as np

cdef class TimeWindowTrades:
    # Используем MemoryView для прямого доступа к C-массиву
    cdef double[:, ::1] data
    cdef long long window_ms
    cdef int maxlen
    cdef int head
    cdef int tail
    cdef int size
    
    # Публичные методы (доступны из Python)
    cpdef void append(self, double ts, double price, double cost)
    cpdef tuple check_alert(self, char* timeframe, dict config, str symbol)
    
    # Внутренние C-методы (Обязательно должны быть здесь, раз они cdef в .pyx)
    cdef int _parse_timeframe(self, char* timeframe)
    cdef str _format_message(self, dict config, str symbol, double price_return, double total_cost)