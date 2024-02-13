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
from fake_useragent import UserAgent

LOGGER = getLogger()


class MsgScraperUsernameConfig(BaseModel):
    block_name: str
    is_class: bool
    is_link: bool


class MsgScraperConfig(BaseModel):
    msg_block_class_name: str
    username_config: MsgScraperUsernameConfig
    msg_text_class_name: str
    date_pattern: str
    forum_link_patterns: list[str] | None


class WebTextScraperOperations:

    @staticmethod
    async def _on_scraping_all_msg_from_url(*,
                                            base_url: str,
                                            saved_path: Path,
                                            msg_config: MsgScraperConfig,
                                            is_stop_404: bool):
        try:
            html_content, isStop = await WebTextScraperOperations._get_html_from_url(url=base_url,
                                                                                     is_stop_404=is_stop_404)

            if isStop:
                raise WebScraperNotFoundError()

            if not html_content:
                raise WebScraperError(f"Not get html content from url - {base_url}")

            base_soup = BeautifulSoup(html_content, 'html.parser')

            await WebTextScraperOperations._parse_and_save_msg_contents_from_html(base_soup=base_soup,
                                                                                  msg_config=msg_config,
                                                                                  url=base_url,
                                                                                  saved_path=saved_path)
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
            response = await aio_to_thread(rt.get, url, headers={'User-agent': UserAgent().random})

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
    async def _parse_and_save_msg_contents_from_html(*,
                                                     base_soup: BeautifulSoup,
                                                     msg_config: MsgScraperConfig,
                                                     url: str,
                                                     saved_path: Path):
        result = None

        try:
            msg_objs = base_soup.find_all(class_=msg_config.msg_block_class_name)

            tasks = []

            for obj in msg_objs:
                task = aio_task(WebTextScraperOperations._parse_msg_to_text(url=url,
                                                                            msg_obj=obj,
                                                                            msg_config=msg_config))
                tasks.append(task)

            result_objs = await aio_gather(*tasks)

            for text, username in result_objs:
                saved_file_path = saved_path / f"{username}.txt"
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
                                 ) -> (str, str):

        result = None

        try:
            text = ""

            forum_url = await WebTextScraperOperations._get_forum_url_from_msg(url=url,
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
            text += f"{forum_url}\n"
            text += f"{msg_text}"
            text += "\n"

            username = await WebTextScraperOperations._get_username_from_msg(msg_obj=msg_obj,
                                                                             username_config=msg_config.username_config)

            result = text, username

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _get_forum_url_from_msg(*,
                                      msg_obj: BeautifulSoup,
                                      forum_link_patterns: [str],
                                      url: str) -> str or None:

        result = None

        try:

            if forum_link_patterns:
                link_obj = await aio_to_thread(msg_obj.find, href=True)

                link = link_obj.get('href')
                text = link_obj.get_text()

                if not link:
                    return

                if not re_search(r'\.\w+(?:\?.*)?$', link, re_I) and (
                        all([re_search(pattern, link, re_I) for pattern in
                             forum_link_patterns])):
                    if link.startswith('/'):
                        result = f"{urljoin(url, link)} ({text})"
                    elif urlparse(link).netloc == urlparse(url).netloc:
                        result = f"{link} ({text})"
            else:
                result = url

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

    @staticmethod
    async def _get_username_from_msg(*,
                                     msg_obj: BeautifulSoup,
                                     username_config: MsgScraperUsernameConfig) -> str or None:
        result = None

        try:

            if username_config.is_class:
                username_obj = await aio_to_thread(msg_obj.find, class_=username_config.block_name)
            else:
                username_obj = await aio_to_thread(msg_obj.find, username_config.block_name)

            if username_config.is_link:
                username_link_obj = await aio_to_thread(username_obj.find, href=True)
                username = username_link_obj.get_text()
            else:
                username = username_obj.get_text()

            result = "".join(x for x in username if (x.isalnum() or x in "._- "))

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result
