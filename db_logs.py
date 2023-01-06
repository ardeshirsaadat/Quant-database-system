import logging
# Improves debuging and quality checks
class DBLogs:
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)
            cls._logger = logging.getLogger()
            
            c_handler = logging.StreamHandler()
            f_handler = logging.FileHandler('applogs.log')
            c_handler.setLevel(logging.DEBUG)
            f_handler.setLevel(logging.DEBUG)
            
            c_handler.setFormatter(logging.Formatter('[%(levelname)s]-%(message)s')) #'[%(levelname)s] %(name)s: [%(threadName)s] %(message)s'))
            f_handler.setFormatter(logging.Formatter('[%(levelname)s]-%(message)s'))
    
            cls._logger.setLevel(logging.DEBUG)
            cls._logger.addHandler(c_handler)
            cls._logger.addHandler(f_handler)
            
        return cls._logger