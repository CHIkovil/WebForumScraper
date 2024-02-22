from fake_useragent import UserAgent
from requests_tor import RequestsTor
from asyncio import to_thread as aio_to_thread
from src.Error.scraper_error import ScraperError
from bs4 import BeautifulSoup
from logging import getLogger
from requests.exceptions import ConnectionError

LOGGER = getLogger()


class ScraperHtmlOperations:

    _RT = RequestsTor(autochange_id=1)

    @staticmethod
    async def get_title_from_html(*, base_obj: BeautifulSoup) -> str or None:
        result = None

        try:
            title_obj = await aio_to_thread(base_obj.find, 'title')

            result = title_obj.text

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def get_html_from_url(*, url: str, is_stop_404: bool) -> (str or None, bool or None):
        result = None
        isStop = False

        try:

            while 1:
                try:
                    response = await aio_to_thread(ScraperHtmlOperations._RT.get, url, headers={'User-agent': UserAgent().random})
                except ConnectionError:
                    continue

                if response.status_code == 200:
                    result = response.text
                elif response.status_code == 404:
                    if is_stop_404:
                        LOGGER.error(f"Requests loop ended with url - {url}")
                        isStop = True
                    else:
                        raise ScraperError("Failed request with response status code 404")
                else:
                    raise ScraperError(f"Failed request with response - {response}")

                break

        except (ScraperError, Exception) as err:
            LOGGER.error(err)
        finally:
            return result, isStop
