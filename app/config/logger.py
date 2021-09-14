class Log4j(object):

    def __init__(self, spark):
        root_class = "clue"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        """Log an warning.
       :param: Error message to write to log
       :return: None
       """
        self.logger.warn(message)

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)

    def debug(self, message):
        """Log debug.
        :param: Debug message to write to log
        :return: None
        """
        self.logger.debug(message)