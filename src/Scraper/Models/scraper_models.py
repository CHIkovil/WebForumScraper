from pydantic import BaseModel
from typing import Optional


class ScraperUserConfig(BaseModel):
    block_name: str
    is_class: bool


class ScraperUserResult(BaseModel):
    url: str
    name: str


class ScraperTopicResult(BaseModel):
    url: str
    name: str


class ScraperMsgConfig(BaseModel):
    msg_block_class_name: str
    msg_text_class_name: str
    quote_block_name: str
    user_config: ScraperUserConfig
    date_pattern: str
    topic_link_patterns: Optional[list[str]]


class ScraperUserMsgResult(BaseModel):
    user: Optional[ScraperUserResult]
    text: Optional[str]


class ScraperMsgResult(BaseModel):
    date: Optional[str]
    answer: Optional[ScraperUserMsgResult]
    questions: Optional[list[ScraperUserMsgResult]]

    topic: Optional[ScraperTopicResult]
