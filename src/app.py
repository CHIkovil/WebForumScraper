from asyncio import run as aio_run
from src.Scraper.web_text_scraper import WebTextScraper
from logging import getLogger
from pathlib import Path
from src.Scraper.web_text_scraper_operations import MsgScraperConfig
from src.Error.web_scraper_error import WebScraperNotFoundError
from sys import exit


LOGGER = getLogger()



if __name__ == "__main__":
    try:
        aio_run(on_script())
    except Exception as error:
        LOGGER.error(error)
