import logging
import os

from scrapy import logformatter


class PoliteLogFormatter(logformatter.LogFormatter):
    def dropped(self, item, exception, response, spider):
        return logformatter.LogFormatterResult(
            level=logging.DEBUG,  # lowering the level from logging.WARNING
            msg="Dropped: %(exception)s" + os.linesep + "%(item)s",
            args={
                "exception": exception,
                "item": item,
            },
        )
