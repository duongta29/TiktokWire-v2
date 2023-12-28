import datetime
from typing import List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException
from post_extractor import PostExtractor
# from utils.datetime_utils import DatetimeUtils
# from utils.log_utils import logger
# from utils.string_utils import StringUtils
import json
import re
import time
import traceback
from post_model import Post
# from utils.format_time import format_time
# from selenium_utils import SeleniumUtils

def extract_timestamp(value):
    if isinstance(value, datetime.datetime):
        value = int(value.timestamp())
    elif isinstance(value, float):
        value = int(value)
    elif not str(value).isdigit():
        try:
            value = int(datetime.datetime.strptime(str(value), "%m/%d/%Y %H:%M:%S").timestamp())
        except ValueError:
            return None
    return value

class PostTikTokExtractor(PostExtractor):
    # POST_AUTHOR_XPATH: str = './/a[not(contains(@href, "group")) and not(@href="#")]'
    def __init__(self, driver: WebDriver, link, source_id):
        super().__init__(driver=driver)
        self.source_id = source_id
        try:
            infor_text = self.driver.find_element(
                By.XPATH, '//*[@id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]').get_attribute('text')
            infor_text = json.loads(infor_text)
            infor_text = infor_text["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
        except:
            try:
                infor_text = self.driver.find_element(By.XPATH, '//*[@id="SIGI_STATE"]').get_attribute('text')
                infor_text = json.loads(infor_text)
                infor_text = infor_text["ItemModule"][self.source_id]
            except:
                driver.refresh()
                print("Refresh page")
                time.sleep(2)
                try: 
                    infor_text = self.driver.find_element(By.XPATH, '//*[@id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]').get_attribute('text')
                    infor_text = json.loads(infor_text)
                    infor_text = infor_text["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
                except:
                    try:
                        infor_text = self.driver.find_element(By.XPATH, '//*[@id="SIGI_STATE"]').get_attribute('text')
                        infor_text = json.loads(infor_text)
                        infor_text = infor_text["ItemModule"][self.source_id]
                    except Exception as e:
                        print(f"Cant crawl {self.link} by Exception {e}")
                    
                    
        self.infor_text = infor_text
        self.link = link

    def extract_post_link(self):
        return self.link

    def extract_post_id(self):
        id = "tt_" + self.source_id
        return id

    def extract_post_time_crawl(self):
        time_crawl = datetime.datetime.now()
        time_crawl = extract_timestamp(time_crawl)
        return time_crawl

    def extract_post_author(self):
        author = self.infor_text["author"]["nickname"]
        return author

    def extract_post_author_link(self):
        author_link = "https://www.tiktok.com/@" + self.infor_text["author"]["uniqueId"]
        return author_link

    def extract_post_author_avatar(self):
        avatar = self.infor_text["author"]["avatarThumb"]
        # avatar = avatar.get_attribute('src')
        return avatar

    def extract_post_created_time(self):
        createTime = self.infor_text["createTime"]
        createTime = extract_timestamp(createTime)
        return createTime

    def extract_post_content(self):
        return ""
    
    def extract_post_title(self):
        title = self.infor_text["desc"]
        return title

    def extract_post_like(self):
        like = int(self.infor_text["stats"]["diggCount"])
        return like

    def extract_post_love(self):
        love = int(self.infor_text["stats"]["collectCount"])
        return love

    def extract_post_comment(self):
        comment = int(self.infor_text["stats"]["commentCount"])
        return comment

    def extract_post_share(self):
        share = int(self.infor_text["stats"]["shareCount"])
        return share

    def extract_post_domain(self):
        domain = "www.tiktok.com"
        return domain

    def extract_post_hashtag(self):
        data = self.infor_text["textExtra"]
        hashtag =  [f'#{item["hashtagName"]}' for item in data]
        return hashtag

    def extract_post_music(self):
        music = "https://www.tiktok.com/music/" + self.infor_text["music"]["title"] + "-" + self.infor_text["music"]["id"]
        return music

    def extract_post_duration(self):
        duration = int(self.infor_text["video"]["duration"])
        return duration

    def extract_post_view(self):
        view = int(self.infor_text["stats"]["playCount"])
        return view

    def extract_post_type(self):
        type = "tiktok video"
        return type

    def extract_post_source_id(self):
        source_id = ""
        return source_id


class PostCommentExtractor(PostExtractor):
    # POST_AUTHOR_XPATH: str = './/a[not(contains(@href, "group")) and not(@href="#")]'
    def __init__(self, driver: WebDriver, comment_dict):
        super().__init__(driver=driver)
        # self.link = link
        # self.source_id = source_id
        # self.comment = count_reply
        self.comment_dict = comment_dict

        # infor_text = self.driver.find_element(By.XPATH,'//*[@id="SIGI_STATE"]').get_attribute('text')
        # infor_text = json.loads(infor_text)
        # self.infor_text = infor_text

    def extract_post_link(self):
        return ""

    def extract_post_id(self):
        id = "tt_" + self.comment_dict["cid"]
        return id

    def extract_post_time_crawl(self):
        time_crawl = datetime.datetime.now()
        time_crawl = extract_timestamp(time_crawl)
        return time_crawl

    def extract_post_author(self):
        author = self.comment_dict["user"]["nickname"]
        return author
    
    def extract_post_title(self):
        return ""

    def extract_post_author_link(self):
        author_link = "https://www.tiktok.com/@" + self.comment_dict["user"]["unique_id"]
        return author_link

    def extract_post_author_avatar(self):
        try:
            avatar = self.comment_dict["user"]["avatar_thumb"]["url_list"][0]
            return avatar
        except:
            return ""

    def extract_post_created_time(self):
        createTime = self.comment_dict["create_time"]
        createTime = extract_timestamp(createTime)
        return createTime
    

    def extract_post_content(self):
        content = self.comment_dict["text"]
        return content

    def extract_post_like(self):
        like = self.comment_dict["digg_count"]
        return like

    def extract_post_love(self):
        return 0

    def extract_post_comment(self):
        comment = self.comment_dict["reply_comment_total"]
        return comment

    def extract_post_share(self):
        return 0

    def extract_post_domain(self):
        domain = "www.tiktok.com"
        return domain

    def extract_post_hashtag(self):
        return []

    def extract_post_music(self):
        return ""

    def extract_post_duration(self):
        return 0

    def extract_post_view(self):
        return 0

    def extract_post_type(self):
        type = "tiktok comment"
        return type

    def extract_post_source_id(self):
        # getIDvid = self.driver.find_element(
        #     By.XPATH, '//*[@class="tiktok-web-player no-controls"]')
        # source_id = "tt_" + ((getIDvid.get_attribute('id')).split('-'))[2]
        # sed = self.link.split('/')
        # source_id = sed[-1]
        source_id = "tt_" + self.comment_dict["aweme_id"]
        return source_id

class PostReplyExtractor(PostExtractor):
    # POST_AUTHOR_XPATH: str = './/a[not(contains(@href, "group")) and not(@href="#")]'
    def __init__(self, driver: WebDriver, reply_dict):
        super().__init__(driver=driver)
        self.reply_dict = reply_dict
       

    def extract_post_link(self):
        return ""

    def extract_post_id(self):
        id = "tt_" + self.reply_dict["reply_id"] + "." + self.reply_dict["cid"]
        return id

    def extract_post_time_crawl(self):
        time_crawl = datetime.datetime.now()
        time_crawl = extract_timestamp(time_crawl)
        return time_crawl

    def extract_post_author(self):
        author = self.reply_dict["user"]["nickname"]
        return author
    
    def extract_post_title(self):
        return ""

    def extract_post_author_link(self):
        author_link = "https://www.tiktok.com/@" + self.reply_dict["user"]["unique_id"]
        return author_link

    def extract_post_author_avatar(self):
        try:
            avatar = self.reply_dict["user"]["avatar_thumb"]["url_list"][0]
            return avatar
        except:
            return ""

    def extract_post_created_time(self):
        createTime = self.reply_dict["create_time"]
        createTime = extract_timestamp(createTime)
        return createTime

    def extract_post_content(self):
        content = self.reply_dict["text"]
        return content

    def extract_post_like(self):
        like = self.reply_dict["digg_count"]
        return like

    def extract_post_love(self):
        return 0

    def extract_post_comment(self):
        # comment = self.comment
        return 0

    def extract_post_share(self):
        return 0

    def extract_post_domain(self):
        domain = "www.tiktok.com"
        return domain

    def extract_post_hashtag(self):
        return []

    def extract_post_music(self):
        return ""

    def extract_post_duration(self):
        return 0

    def extract_post_view(self):
        return 0

    def extract_post_type(self):
        type = "tiktok comment"
        return type

    def extract_post_source_id(self):
        source_id = "tt_" + self.reply_dict["aweme_id"]
        return source_id