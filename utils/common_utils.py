import random
import time

class CommonUtils:
    @staticmethod
    def sleep_random_in_range(a: float, b: float) -> float:
        sleep_time = random.uniform(a, b)
        time.sleep(sleep_time)
        return sleep_time