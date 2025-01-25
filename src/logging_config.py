import logging


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s [Module:%(filename)s:%(lineno)d, func:%(funcName)s] %(message)s",
)

logger = logging.getLogger()
