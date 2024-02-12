from pathlib import Path
from logging import getLogger
from src.Scraper.web_text_scraper_operations import MsgScraperConfig
from src.Scraper.web_text_scraper_operations import WebTextScraperOperations
from src.Error.web_scraper_error import WebScraperNotFoundError

LOGGER = getLogger()


class WebTextScraper(WebTextScraperOperations):
    def __init__(self, *,
                 url: str,
                 saved_path: Path,
                 msg_config: MsgScraperConfig,
                 is_stop_404: bool):

        self._url = url
        self._saved_path = saved_path
        self._msg_config = msg_config
        self._is_stop_404 = is_stop_404

    async def run(self):
        try:
            self._saved_path.mkdir(parents=True, exist_ok=True)
            await self._on_scraping_all_msg_from_url(base_url=self._url,
                                                     saved_path=self._saved_path,
                                                     msg_config=self._msg_config,
                                                     is_stop_404=self._is_stop_404)
        except WebScraperNotFoundError:
            raise
        except Exception as err:
            LOGGER.error(err)
