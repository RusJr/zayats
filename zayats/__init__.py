from zayats.cosumer import RabbitConsumer
from zayats.publisher import RabbitPublisher


import logging.config


log_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {'zayats_format': {'format': '[%(levelname)s] [%(asctime)s] [%(name)s] %(message)s',
                                     'datefmt': '%d/%m/%Y %H:%M:%S'}},
    'handlers': {'zayats_console': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'zayats_format'}},
    'loggers': {'RabbitPublisher': {'handlers': ['zayats_console'], 'propagate': False, 'level': 'DEBUG'},
                'RabbitConsumer': {'handlers': ['zayats_console'], 'propagate': False, 'level': 'DEBUG'}, }
}

logging.config.dictConfig(log_dict)
