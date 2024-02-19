from asyncio import run as aio_run
from src.Scraper.scraper import Scraper
from logging import getLogger
from pathlib import Path
from src.Scraper.Models.scraper_models import ScraperMsgConfig, ScraperUserConfig, ScraperMsgResult
from src.Error.scraper_error import ScraperNotFoundError
from sys import exit
from xlrd import open_workbook
from xlwt import Workbook
from xlutils.copy import copy
from asyncio import to_thread as aio_to_thread
from enum import Enum
from fake_useragent import UserAgent
from requests_tor import RequestsTor
from src.Scraper.Operations.scraper_msg_operations import ScraperMsgOperations
from bs4 import BeautifulSoup
from aiofiles import open as aio_open

LOGGER = getLogger()




if __name__ == "__main__":
    try:
        aio_run(search_and_save_users())
    except KeyboardInterrupt:
        exit(0)
    except Exception as error:
        LOGGER.error(error)
