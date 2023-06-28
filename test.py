from ulog import Logger, INFO
import time


# cat /proc/sys/net/unix/max_dgram_qlen gives 128
# gah, I think I acidentally set it with sudo sysctl -w net.unix.max_dgram_qlen=128
# I wonder what it was before, I thought it was 512


logger = Logger('foo', 'test.log', stdout_level=None, stderr_level=None)
i = 0

data = '0' * 100
t0 = time.perf_counter()
try:
    while True:
        i += 1
        logger.info(data)
finally:
    print(i)
    print((time.perf_counter() - t0) / i)
