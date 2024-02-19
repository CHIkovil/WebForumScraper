from asyncio import to_thread as aio_to_thread
from logging import getLogger
from bs4 import BeautifulSoup
from re import (search as re_search,
                I as re_I)
from urllib.parse import urlparse, urljoin
from re import compile as re_compile
from src.Scraper.Models.scraper_models import (ScraperUserConfig, ScraperUserResult, ScraperTopicResult)
from asyncio import (gather as aio_gather)

LOGGER = getLogger()


class ScraperMsgOperations:

    @staticmethod
    async def get_topic_from_msg(*,
                                 msg_obj: BeautifulSoup,
                                 topic_link_patterns: [str],
                                 base_url: str) -> str or None:

        result = None

        try:

            link_obj = await aio_to_thread(msg_obj.find, href=True)

            link = link_obj.get('href')
            text = link_obj.get_text()

            if not link:
                return

            searched_results = await aio_gather(*[aio_to_thread(re_search, pattern, link, re_I) for pattern in
                                                  topic_link_patterns])

            if not re_search(r'\.\w+(?:\?.*)?$', link, re_I) and (
                    all(searched_results)):
                if link.startswith('/'):
                    result = ScraperTopicResult(url=urljoin(base_url, link),
                                                name=text)

                elif urlparse(link).netloc == urlparse(base_url).netloc:
                    result = ScraperTopicResult(url=link,
                                                name=text)

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def get_date_from_msg(*,
                                msg_obj: BeautifulSoup,
                                date_pattern: str) -> str or None:
        result = None

        try:

            html_text = msg_obj.prettify()

            pattern = re_compile(date_pattern)

            match_obj = await aio_to_thread(pattern.search, html_text)

            if match_obj:
                result = match_obj.group(1).strip()

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def get_user_from_msg(*,
                                msg_obj: BeautifulSoup,
                                base_url: str,
                                user_config: ScraperUserConfig) -> ScraperUserResult or None:
        result = None

        try:
            if user_config.is_class:
                user_name_obj = await aio_to_thread(msg_obj.find, class_=user_config.block_name)
            else:
                user_name_obj = await aio_to_thread(msg_obj.find, user_config.block_name)

            user_name_link_obj = await aio_to_thread(user_name_obj.find, href=True)
            link = user_name_link_obj.get('href')
            text = user_name_link_obj.get_text()

            if link.startswith('/'):
                result = ScraperUserResult(url=urljoin(base_url, link),
                                           name=text)
            elif urlparse(link).netloc == urlparse(base_url).netloc:
                result = ScraperUserResult(url=link,
                                           name=text)
        except Exception as err:
            LOGGER.error(err)

        finally:
            return result
