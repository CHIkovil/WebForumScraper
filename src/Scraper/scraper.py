from logging import getLogger
from src.Scraper.Models.scraper_models import ScraperMsgConfig, ScraperMsgResult
from src.Scraper.scraper_operations import ScraperOperations
from src.Error.scraper_error import ScraperNotFoundError

LOGGER = getLogger()


class Scraper(ScraperOperations):
    def __init__(self, *,
                 url: str,
                 msg_config: ScraperMsgConfig,
                 is_stop_404: bool):

        self._url = url
        self._msg_config = msg_config
        self._is_stop_404 = is_stop_404

    async def run(self) -> [ScraperMsgResult]:
        try:
            result = await self.on_scraping_all_msg_from_url(base_url=self._url,
                                                              msg_config=self._msg_config,
                                                              is_stop_404=self._is_stop_404)

            return result
        except ScraperNotFoundError:
            raise
        except Exception as err:
            LOGGER.error(err)
