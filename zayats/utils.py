import logging.config


def set_logger(logger_name: str, level='INFO', disable_existing_loggers=False) -> None:
    log_dict = {
        'version': 1,
        'disable_existing_loggers': disable_existing_loggers,
        'formatters': {'zayats_format': {'format': '[%(levelname)s] [%(asctime)s] [%(name)s] %(message)s',
                                         'datefmt': '%d/%m/%Y %H:%M:%S'}},
        'handlers': {'zayats_cons': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'zayats_format'}},
        'loggers': {logger_name: {'handlers': ['zayats_cons'], 'propagate': False, 'level': level}}
    }

    logging.config.dictConfig(log_dict)
