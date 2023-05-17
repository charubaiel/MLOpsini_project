import logging
from functools import wraps
import datetime

logging.basicConfig(
    format=u'#%(levelname)-s [%(name)s] [%(asctime)s]  %(message)s',
    level=logging.INFO)

_logger = logging.getLogger('BASE')


def logger(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        _logger.info(f'START {func.__name__}')
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now() - start_time
        _logger.info(f'END   {func.__name__} in {end_time}')
        return result

    return wrapper
