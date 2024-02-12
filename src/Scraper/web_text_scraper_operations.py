from requests_tor import RequestsTor
from asyncio import to_thread as aio_to_thread
from aiofiles import open as aio_open
from pathlib import Path
from logging import getLogger
from src.Error.web_scraper_error import WebScraperError, WebScraperNotFoundError
from bs4 import BeautifulSoup
from pydantic import BaseModel
from re import (search as re_search,
                I as re_I)
from urllib.parse import urlparse, urljoin
from asyncio import (create_task as aio_task,
                     gather as aio_gather)
from re import compile as re_compile

LOGGER = getLogger()


class MsgScraperConfig(BaseModel):
    msg_block_class_name: str
    msg_text_class_name: str
    date_pattern: str
    forum_link_patterns: list[str]


class WebTextScraperOperations:

    @staticmethod
    async def _on_scraping_all_msg_from_url(*,
                                            base_url: str,
                                            saved_path: Path,
                                            msg_config: MsgScraperConfig,
                                            is_stop_404: bool):
        try:
            html_content, isStop = await WebTextScraperOperations._get_html_from_url(url=base_url, is_stop_404=is_stop_404)

            if isStop:
                raise WebScraperNotFoundError()

            if not html_content:
                raise WebScraperError(f"Not get html content from url - {base_url}")

            base_soup = BeautifulSoup(html_content, 'html.parser')

            title = await WebTextScraperOperations._get_title_from_html(base_soup=base_soup)

            saved_file_path = saved_path / f"{title}.txt"

            await WebTextScraperOperations._parse_and_save_msg_contents_from_html(base_soup=base_soup,
                                                                                  msg_config=msg_config,
                                                                                  url=base_url,
                                                                                  saved_file_path=saved_file_path)
        except WebScraperNotFoundError:
            raise
        except (Exception, WebScraperError) as err:
            LOGGER.error(err)

    @staticmethod
    async def _get_html_from_url(*, url: str, is_stop_404: bool) -> (str or None, bool or None):
        result = None
        isStop = False

        try:
            rt = RequestsTor(autochange_id=1)
            response = await aio_to_thread(rt.get, url)

            if response.status_code == 200:
                result = response.text
            elif response.status_code == 404:
                if is_stop_404:
                    LOGGER.error(f"Requests loop ended with url - {url}")
                    isStop = True
                else:
                    raise WebScraperError("Failed request with response status code 404")
            else:
                raise WebScraperError(f"Failed request with response - {response}")
        except (WebScraperError, Exception) as err:
            LOGGER.error(err)
        finally:
            return result, isStop

    @staticmethod
    async def _get_title_from_html(*, base_soup: BeautifulSoup) -> str or None:
        result = None

        try:
            title_tag = await aio_to_thread(base_soup.find, 'title')

            result = "".join(x for x in title_tag.text if (x.isalnum() or x in "._- "))

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _parse_and_save_msg_contents_from_html(*,
                                                     base_soup: BeautifulSoup,
                                                     msg_config: MsgScraperConfig,
                                                     url: str,
                                                     saved_file_path: Path):
        result = None

        try:
            msg_objs = base_soup.find_all(class_=msg_config.msg_block_class_name)

            tasks = []

            for obj in msg_objs:
                task = aio_task(WebTextScraperOperations._parse_msg_to_text(url=url,
                                                                            msg_obj=obj,
                                                                            msg_config=msg_config))
                tasks.append(task)

            msg_texts = await aio_gather(*tasks)

            for text in msg_texts:
                async with aio_open(saved_file_path, 'a' if saved_file_path.exists() else 'w') as f:
                    await f.write(text)

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _parse_msg_to_text(*,
                                 url: str,
                                 msg_obj: BeautifulSoup,
                                 msg_config: MsgScraperConfig
                                 ) -> str:

        result = None

        try:
            text = ""

            forum_urls = await WebTextScraperOperations._get_forum_urls_from_msg(url=url,
                                                                                 forum_link_patterns=msg_config.forum_link_patterns,
                                                                                 msg_obj=msg_obj)

            msg_text_objs = await aio_to_thread(msg_obj.find_all, class_=msg_config.msg_text_class_name)

            msg_text = ""

            for obj in msg_text_objs:
                msg_text += f"{obj.get_text().strip()}\n"

            date = await WebTextScraperOperations._get_date_from_msg(msg_obj=msg_obj,
                                                                     date_pattern=msg_config.date_pattern)
            text += "\n"
            text += f"{date}\n"
            text += f"{' '.join(forum_urls)}\n"
            text += f"{msg_text}"
            text += "\n"

            result = text

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _get_forum_urls_from_msg(*,
                                       msg_obj: BeautifulSoup,
                                       forum_link_patterns: [str],
                                       url: str) -> [str] or None:

        result = None

        try:
            link_objs = await aio_to_thread(msg_obj.find_all, href=True)

            forum_urls = []

            for obj in link_objs:
                link = obj.get('href')
                text = obj.get_text()

                if not link:
                    continue

                if not re_search(r'\.\w+(?:\?.*)?$', link, re_I) and (
                        all([re_search(pattern, link, re_I) for pattern in
                             forum_link_patterns])):
                    if link.startswith('/'):
                        forum_urls.append(f"{urljoin(url, link)} ({text})")
                    elif urlparse(link).netloc == urlparse(url).netloc:
                        forum_urls.append(f"{link} ({text})")

            result = forum_urls

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _get_date_from_msg(*,
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
