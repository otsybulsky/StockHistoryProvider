from decouple import config
from logging_config import logger


SOME_SECRET = config("SOME_SECRET", default=None)
assert SOME_SECRET is not None
SOME_SECRET = str(SOME_SECRET).strip()


if __name__ == "__main__":
    logger.info("Environment variables:")
    logger.debug(f"SOME_SECRET={SOME_SECRET} ({type(SOME_SECRET)})")
