import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s [Module:%(filename)s:%(lineno)d, func:%(funcName)s] %(message)s",
)

logger = logging.getLogger()


# Function for logging execution times with high accuracy
def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time_sec = end_time - start_time  # Execution time in seconds
        # execution_time_ns = execution_time_sec * 1_000_000_000  # Conversion to nanoseconds
        logger.info(
            f"Function '{func.__name__}' was performed for {execution_time_sec:.4f} seconds."
        )
        return result

    return wrapper
