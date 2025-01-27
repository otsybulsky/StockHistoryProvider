from decouple import config
from logging_config import logger


def load_variable(variable_name):
    val = config(variable_name, default=None)
    assert val is not None
    val = str(val).strip()
    return val


SOME_SECRET = load_variable("SOME_SECRET")
FLAT_DATA_FOLDER = load_variable("FLAT_DATA_FOLDER")

if __name__ == "__main__":
    logger.info("Environment variables:")
    logger.debug(f"SOME_SECRET={SOME_SECRET} ({type(SOME_SECRET)})")
