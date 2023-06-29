from ulog import Logger
import time

logger = Logger('foo', 'test.log', stdout_level=None, stderr_level=None)
i = 0

data = '0' * 100
t0 = time.perf_counter()
try:
    for i in range(10000):
        logger.info(data)
finally:
    print(i)
    print(f"{1e6 * (time.perf_counter() - t0) / i:.2f} us per record")
