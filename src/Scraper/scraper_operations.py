from asyncio import to_thread as aio_to_thread
from logging import getLogger
from src.Error.scraper_error import ScraperError, ScraperNotFoundError
from bs4 import BeautifulSoup
from asyncio import (gather as aio_gather)
from src.Scraper.Operations.scraper_html_operations import ScraperHtmlOperations
from src.Scraper.Operations.scraper_msg_operations import ScraperMsgOperations
from src.Scraper.Models.scraper_models import (ScraperMsgConfig,
                                               ScraperMsgResult,
                                               ScraperUserResult,
                                               ScraperUserMsgResult,
                                               ScraperTopicResult)
from re import (search as re_search,
                escape as re_escape,
                sub as re_sub,
                I as re_I)

LOGGER = getLogger()


class ScraperOperations(ScraperHtmlOperations,
                        ScraperMsgOperations):

    @staticmethod
    async def on_scraping_all_msg_from_url(*,
                                           base_url: str,
                                           msg_config: ScraperMsgConfig,
                                           is_stop_404: bool) -> [ScraperMsgResult]:
        try:
            html_content, isStop = await ScraperOperations.get_html_from_url(url=base_url,
                                                                             is_stop_404=is_stop_404)

            if isStop:
                raise ScraperNotFoundError()

            if not html_content:
                raise ScraperError(f"Not get html content from url - {base_url}")

            base_obj = BeautifulSoup(html_content, r'html.parser')

            result = await ScraperOperations._parse_and_save_msg_contents_from_html(base_obj=base_obj,
                                                                                    msg_config=msg_config,
                                                                                    url=base_url)

            return result

        except ScraperNotFoundError:
            raise
        except (Exception, ScraperError) as err:
            LOGGER.error(err)

    @staticmethod
    async def _parse_and_save_msg_contents_from_html(*,
                                                     base_obj: BeautifulSoup,
                                                     msg_config: ScraperMsgConfig,
                                                     url: str) -> [ScraperMsgResult]:
        result = None

        try:
            msg_objs = await aio_to_thread(base_obj.find_all, class_=msg_config.msg_block_class_name)

            result_objs = []

            for index, obj in enumerate(msg_objs, start=1):
                result = await ScraperOperations._parse_msg_to_text(base_url=url,
                                                                    base_obj=base_obj,
                                                                    msg_obj=obj,
                                                                    msg_config=msg_config)

                if result:
                    result_objs.append(result)

                else:
                    raise ScraperError(f"Not parsed msg {index} from - {url}")

            result = result_objs

        except (Exception, ScraperError) as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _parse_msg_to_text(*,
                                 base_url: str,
                                 base_obj: BeautifulSoup,
                                 msg_obj: BeautifulSoup,
                                 msg_config: ScraperMsgConfig
                                 ) -> ScraperMsgResult:

        result = None

        try:

            # Msg
            msg_text_obj = await aio_to_thread(msg_obj.find, class_=msg_config.msg_text_class_name)
            answer_text = msg_text_obj.get_text().strip()

            # Quote

            quote_objs = await aio_to_thread(msg_obj.find_all, msg_config.quote_block_name)
            quote_patterns = [quote_obj.get_text().strip() for quote_obj in quote_objs] if len(
                quote_objs) != 0 else None

            # Topic

            if msg_config.topic_link_patterns:
                topic = await ScraperOperations.get_topic_from_msg(base_url=base_url,
                                                                   topic_link_patterns=msg_config.topic_link_patterns,
                                                                   msg_obj=msg_obj)

            else:
                topic_name = await ScraperOperations.get_title_from_html(base_obj=base_obj)

                topic = ScraperTopicResult(url=base_url,
                                           name=topic_name)

            # Date
            date = await ScraperOperations.get_date_from_msg(msg_obj=msg_obj,
                                                             date_pattern=msg_config.date_pattern)

            # User
            answer_user: ScraperUserResult = await ScraperOperations.get_user_from_msg(msg_obj=msg_obj,
                                                                                       base_url=base_url,
                                                                                       user_config=msg_config.user_config)

            # Search questions and full answer
            questions = None

            if topic:
                searched_result = await ScraperOperations._search_questions_from_topic(base_url=base_url,
                                                                                       topic_url=topic.url,
                                                                                       searching_msg_pattern=answer_text,
                                                                                       searching_quote_patterns=quote_patterns,
                                                                                       msg_config=msg_config)
                if searched_result:
                    answer_text, questions = searched_result

            result = ScraperMsgResult(date=date,
                                      topic=topic,
                                      answer=ScraperUserMsgResult(user=answer_user,
                                                                  text=answer_text),
                                      questions=questions)
        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _search_questions_from_topic(*,
                                           base_url: str,
                                           topic_url: str,
                                           searching_msg_pattern: str,
                                           searching_quote_patterns: [str] or None,
                                           msg_config: ScraperMsgConfig) -> (str,
                                                                             ScraperUserMsgResult) or None:
        result = None

        try:
            searching_patterns = [searching_msg_pattern]

            if searching_quote_patterns:
                searching_patterns.extend(searching_quote_patterns)

            searching_msg_pattern, searching_quote_patterns = await ScraperOperations._validate_searching_patterns(
                patterns=searching_patterns)

            topic_html_content, _ = await ScraperOperations.get_html_from_url(url=topic_url,
                                                                              is_stop_404=False)
            topic_obj = BeautifulSoup(topic_html_content, 'html.parser')
            msg_objs = await aio_to_thread(topic_obj.find_all, class_=msg_config.msg_block_class_name)

            answer_text_index, answer_text, questions = await ScraperOperations._search_quote_questions_from_msgs(
                base_url=base_url,
                msg_objs=msg_objs,
                msg_config=msg_config,
                msg_pattern=searching_msg_pattern,
                quote_patterns=searching_quote_patterns)
            if answer_text_index:
                if len(questions) == 0 and answer_text_index >= 1:
                    previous_msg_obj = msg_objs[answer_text_index - 1]
                    previous_msg_text_obj = await aio_to_thread(previous_msg_obj.find,
                                                                class_=msg_config.msg_text_class_name)

                    question_text = previous_msg_text_obj.get_text().strip()
                    question_user = await ScraperOperations.get_user_from_msg(msg_obj=previous_msg_obj,
                                                                              base_url=base_url,
                                                                              user_config=msg_config.user_config)
                    question = ScraperUserMsgResult(user=question_user,
                                                    text=question_text)
                    questions.append(question)

            result = (answer_text if answer_text else searching_msg_pattern,
                      questions if len(questions) != 0 else None)

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _validate_searching_patterns(*, patterns: [str]):
        result = None

        try:
            validated_patterns = []

            for pattern in patterns:
                text = pattern[:pattern.rfind('…')] if pattern.endswith('…') else pattern
                text = re_escape(re_sub(r'\s+', '', text))

                validated_patterns.append(text)

            result = validated_patterns[0], validated_patterns[1:] if len(validated_patterns) > 1 else None

        except Exception as err:
            LOGGER.error(err)

        finally:
            return result

    @staticmethod
    async def _search_quote_questions_from_msgs(*,
                                                base_url: str,
                                                msg_objs: [BeautifulSoup],
                                                msg_config: ScraperMsgConfig,
                                                msg_pattern: str,
                                                quote_patterns: [str] or None
                                                ):
        answer_text_index = None
        answer_text = None
        questions = []

        try:
            for index, msg_obj in enumerate(msg_objs):
                msg_text_obj = await aio_to_thread(msg_obj.find, class_=msg_config.msg_text_class_name)
                msg_text = msg_text_obj.get_text().strip()

                if quote_patterns:
                    quote_objs = await aio_to_thread(msg_obj.find_all, msg_config.quote_block_name)
                    quote_text = " ".join([obj.get_text().strip() for obj in quote_objs])

                    quote_results = await aio_gather(
                        *[aio_to_thread(re_search, pattern, re_sub(r'\s+', '', quote_text), re_I) for pattern in
                          quote_patterns])

                    text_results = await aio_gather(
                        *[aio_to_thread(re_search, pattern, re_sub(r'\s+', '', msg_text), re_I) for pattern in
                          quote_patterns])

                    if all(quote_results):
                        answer_text_index = index
                        answer_text = msg_text

                    elif any(text_results):
                        question_text = msg_text
                        question_user = await ScraperOperations.get_user_from_msg(msg_obj=msg_obj,
                                                                                  base_url=base_url,
                                                                                  user_config=msg_config.user_config)
                        question = ScraperUserMsgResult(user=question_user,
                                                        text=question_text)
                        questions.append(question)
                else:
                    searched_result = await aio_to_thread(re_search, msg_pattern, re_sub(r'\s+', '', msg_text),
                                                          re_I)

                    if searched_result:
                        answer_text_index = index
                        answer_text = msg_text

        except Exception as err:
            LOGGER.error(err)

        finally:
            return answer_text_index, answer_text, questions
