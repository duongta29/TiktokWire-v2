from seleniumwire import webdriver
from selenium.webdriver.common.by import By
import time
import json
import schedule
from seleniumwire.utils import decode as sw_decode
import time
import config 
import json
import pickle
from typing import List
from post_tiktok_etractor import PostTikTokExtractor, PostCommentExtractor, PostReplyExtractor
from utils.common_utils import CommonUtils
import config
import captcha
from kafka import KafkaProducer
from selenium.webdriver.common.action_chains import ActionChains
from process_data import *
from login import TiktokLogin
from es import *
from datetime import datetime, timedelta
import clipboard
from get_link_from_android import *
import requests
producer = KafkaProducer(bootstrap_servers=["192.168.143.54:9092"])
api_address='http://172.168.143.54:8668'




### OTHER ###
with open(config.config_path, "r", encoding='latin-1') as f:
    data = f.read()
    data_config = json.loads(data)
    option = data_config["mode"]["name"]
    listArgument = data_config["listArgument"]

chrome_options = webdriver.ChromeOptions()
for item in listArgument:
        chrome_options.add_argument(item)
chrome_options.add_argument(f'--proxy-server={config.proxy}')


class CrawlManage(object):
    XPATH_VIDEO_SEARCH = '//*[contains(@class, "DivItemContainerForSearch")]'
    XPATH_VIDEO_OTHER = '//*[contains(@class, "DivItemContainerV2")]'
    # XPATH_VIDEO_OTHER = '//*[@class="tiktok-x6y88p-DivItemContainerV2 e19c29qe9"]'
    XPATH_VIDEO_PAGE = '//*[@data-e2e="user-post-item-desc"]'

    def __init__(self, driver = webdriver.Chrome(options=chrome_options) , config=config) -> None:
        self.driver = driver
        self.config = config
        self.actions = ActionChains(self.driver)
        self.link = None
        self.comments = []
        self.reply = []

    def parse_keyword(self) -> List[str]:
        keyword_list: List[str] = []
        with open(self.config.config_path, "r", encoding='utf-8') as f:
            data = f.read()
            keyword_list_raw = json.loads(data)
            if option == "search_user":
                keyword_list = keyword_list_raw["mode"][f"list_page"]
            elif option == "search_post_android" or option == "search_post":
                keyword_list = keyword_list_raw["mode"]["keyword"]
        return keyword_list

    def check_login_div(self):
        print("Check login div")
        try:
            button = self.driver.find_element(By.XPATH, '//*[@data-e2e="modal-close-inner-button"]')
            button.click()
        except Exception as e:
            print(f"No login div or Exception {e}")

    def crawl_comment(self):
        comments = []
        list_comment = self.comments
        self.comments = []
        list_replies = self.reply
        self.reply = []
        for comment_dict in list_comment:
            comment_extractor: PostCommentExtractor = PostCommentExtractor(driver=self.driver, comment_dict = comment_dict)
            comment = comment_extractor.extract()
            comments.append(comment)
            write_post_to_file(post=comment)
            try: 
                if comment_dict["reply_comment"] is not None:
                    list_reply = comment_dict["reply_comment"]
                    for reply_dict in list_reply:
                        reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
                        reply = reply_extractor.extract()
                        comments.append(reply)
                        write_post_to_file(post=reply)
            except Exception as e:
                 print(f"Error to crawl comment with Exception {e}")
        for reply_dict in list_replies:
            reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
            reply = reply_extractor.extract()
            comments.append(reply)
            write_post_to_file(post = reply)
        return comments
        
    def interceptor_post(self, request, response):
        if "comment/list/?WebIdLastTime" in request.url:
            data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
            try:
                data = data.decode("utf8")
            except:
                pass
            data = json.loads(data)
            list_comment = data["comments"]
            if list_comment is not None:
                for comment in list_comment:
                    self.comments.append(comment) 
        if "comment/list/reply/?WebIdLastTime" in request.url:
            data = sw_decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
            try:
                data = data.decode("utf8")
            except:
                pass
            data = json.loads(data)
            list_reply = data["comments"]
            if list_reply is not None:
                for reply in list_reply:
                    self.reply.append(reply)

    def scroll_comment(self):
            cmts = []
            check = 1
            while (len(cmts) != check):
                check = len(cmts)
                cmts = []
                self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                cmts = self.driver.find_elements(
                        By.XPATH, '//*[contains(@class, "DivCommentItemContainer")]')
                time.sleep(2)
            try:
                    self.driver.execute_script("window.scrollTo(0, 0);")
            except:
                    pass
            for cmt in cmts:
                BOOL = True
                while(BOOL):
                    try:
                        # reply_div = cmt.find_element(By.XPATH, './/div[contains(@class, "DivReplyActionContainer")]')
                        reply_button = cmt.find_element(By.XPATH, './/p[contains(@data-e2e, "view-more-")]')
                        reply_button.click()
                        self.driver.execute_script("window.scrollTo(0, 1000);")
                        time.sleep(2)
                    except Exception as e:
                        BOOL = False
    
    def get_links(self, table_name, object_id):
        url = f"{api_address}/get-links/{table_name}/{object_id}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                print(f"Error: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error: {str(e)}")

    # API insert link đã crawl vào db
    def insert(self,table_name, object_id, links):
        if isinstance(links, list):
            links = ",".join(links)
        url = f"{api_address}/insert/{table_name}/{object_id}?new_links={links}"
        try:
            response = requests.post(url)
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                print(f"Error: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error: {str(e)}")

            
    def crawl_post(self, link):
        self.driver.response_interceptor = self.interceptor_post
        segments = link.split("/")
        source_id = segments[-1]
        try:
            posts=[]
            self.comments =[]
            self.driver.get(link) 
            self.driver.implicitly_wait(5)
            # self.check_login_div()
            # time.sleep(3)
            print(f" >>> Crawling: {link} ...")
            post_extractor: PostTikTokExtractor = PostTikTokExtractor(driver=self.driver, link=link, source_id=source_id)
            post = post_extractor.extract()
            retry_time = 0
            def retry_extract(post, retry_time):
                while not post.is_valid():
                    post = post_extractor.extract()
                    if retry_time > 0:
                        print(
                            f"Try to extract post {retry_time} times {str(post)}")
                        slept_time = CommonUtils.sleep_random_in_range(1, 5)
                        print(f"Slept {slept_time}")
                    retry_time = retry_time + 1
                    if retry_time > 20:
                        print("Retried 20 times, skip post")
                        break
                return
            retry_extract(post, retry_time)
            posts.append(post)
            if option == "update_post":
                #sroll to crawl comment
                self.scroll_comment()
            else:
                # update post to data_crawled and write post to result
                page_name = link.split('@')[1].split('/')[0]
                video_id = link.split('/')[-1]
                # self.insert(table_name="tiktok_video",object_id=page_name, links=[video_id]) 
                update_json_file(file_path="data_crawled.json", new_link=link)
            # crawl cmt and push kafka
            write_post_to_file(post=post)
            if posts != [] : 
                comments = self.crawl_comment()
                del self.driver.response_interceptor
                # print("push kafka")
                try:
                    push_kafka = self.push_kafka(posts,comments)
                    if push_kafka == 1:
                        print("Done push kafka")
                except Exception as e:
                    print(f"Error to push kafka Exception: {e}")
        except Exception as e:
            print(f"Error to crawl_post Exception: {e}")
            captcha.check_captcha(self.driver)
            # check link exist
            try:
                not_have = self.driver.find_element("xpath", '//*[@id="main-content-video_detail"]/div/div')
                not_have = not_have.find_element(By.TAG_NAME, 'p')   
                not_have = not_have.text
                if not_have == "Video currently unavailable":
                    print("Video currently unavailable")
                else:
                    return self.crawl_post(link)
            except:
                return self.crawl_post(link)
            
    def push_kafka(self, posts, comments):
        if option == "update_post":
            topic = "osint-posts-update"
        else:
            topic = "lnmxh"
        if len(posts) > 0:
                bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
                producer.send(topic, bytes_obj)
                if len(comments) > 0:
                    bytes_obj = pickle.dumps([ob.__dict__ for ob in comments])
                    producer.send(topic, bytes_obj)
                return 1
        else:
                return 0
       
    def get_es(self):
        range_date = data_config["mode"]["range_date"]
        format_str = "%m/%d/%Y %H:%M:%S"
        now = datetime.now()
        link_to_update = []
        for range_value in range_date:
            gte = now - timedelta(days=int(range_value))
            lte = now - timedelta(days=int(range_value)-1)
            gte = gte.replace(hour=0, minute=0, second=0, microsecond=0)
            lte = lte.replace(hour=0, minute=0, second=0, microsecond=0)
            gte_str = gte.strftime(format_str)
            lte_str = lte.strftime(format_str)
            link = get_link_es(type_list=['tiktok video'], gte=gte_str, lte=lte_str)
            link_to_update.extend(link)
        return link_to_update

    def update_post(self):
        link_list = self.get_es()
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=23, minutes=15)
        for link in link_list:
            if datetime.now() < end_time:
                start = time.time()
                self.crawl_post(link)
                end = time.time()
                print(f"Time for video {link} is {end - start}")
            else:
                break
    
    def run(self):
        self.driver.get("https://www.tiktok.com/")
        time.sleep(2)
        captcha.check_captcha(self.driver)
        ttLogin = TiktokLogin(self.driver, username = "babysunny2906")
        try:
            ttLogin.loginTiktokwithCookie()
        except Exception as e:
            print("Retry to login Exception {e}")
            print("Try login with pass and save new cookie")
            new_cookies = ttLogin.save_cookie()
            if new_cookies:
                print("Done save new cookie")
        print("Start crawl")
        key_search = []
        # time.sleep(3)
        if option == "update_post":
            start_time_run = data_config["mode"]["start_time_run"]
            schedule.every().day.at(start_time_run).do(self.update_post)
            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            key_search = self.parse_keyword()
            for key in key_search:
                link_list = self.get_link_list(key)
                for link in link_list:
                    start = time.time()
                    self.crawl_post(link)
                    end = time.time()
                    print(f"Time for video {link} is {end - start}")
            time.sleep(30*60)
            return self.run()
        
    def shorten_links(tiktok_links):
        link_dict = {}
        for link in tiktok_links:
            # Tách tên trang và ID video từ liên kết
            page_name = link.split('@')[1].split('/')[0]
            video_id = link.split('/')[-1]
            # Kiểm tra xem trang đã tồn tại trong từ điển chưa
            if page_name in link_dict:
                # Nếu đã tồn tại, thêm ID video vào danh sách
                link_dict[page_name].append(video_id)
            else:
                # Nếu chưa tồn tại, tạo một cặp key-value mới
                link_dict[page_name] = [video_id]
    
    def check_link_crawled(self, link):
        # id_check = {}
        # page_name = link.split('@')[1].split('/')[0]
        # video_id = link.split('/')[-1]
        # data_crawled = self.get_links(table_name="tiktok_video", object_id= page_name)
        # if data_crawled:
        #     if video_id in data_crawled["links"]:
        #         return True
        #     else:
        #         return False
        # else:
        #     return False
        return False

    def scroll(self, xpath):
        vidList = []
        # time.sleep(3)
        # with open('data_crawled.json', 'r') as file:
        #     data_crawled = json.load(file)
        try:
            captcha.check_captcha(self.driver)
        except:
            pass
        count = 1
        vid_list_elem = []
        if option == "search_user":
            try:
                no_post = self.driver.find_element(By.XPATH, '//*[@class="tiktok-1ovqurc-PTitle emuynwa1"]').text()
                print("No post")
            except:
                no_post =""
            while(len(vid_list_elem) != count and len(vid_list_elem) < self.config.count_of_vid and no_post != "No content"):
                # data-e2e="search-common-link"
                count = len(vid_list_elem)
                try:
                    vid_list_elem = self.driver.find_elements(By.XPATH, xpath)
                except:
                    vid_list_elem = self.driver.find_elements(By.XPATH, xpath)
                i = 0
                for vid in vid_list_elem:
                    link = vid.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    if link in vidList:
                        continue
                    check = self.check_link_crawled(link)
                    if check:
                        i += 1
                        continue
                    if check and i > 3:
                        break
                    else:
                        vidList.append(link)
                # print("len vid: ", len(vid_list_elem))
                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
        else:
        # time.sleep(3)
            while(len(vid_list_elem) != count and len(vid_list_elem) < self.config.count_of_vid):
                    # data-e2e="search-common-link"
                    count = len(vid_list_elem)
                    try:
                        vid_list_elem = self.driver.find_elements(By.XPATH, xpath)
                    except:
                        vid_list_elem = self.driver.find_elements(By.XPATH, xpath)
                    # print("len vid: ", len(vid_list_elem))
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
            for vid in vid_list_elem:
                link = vid.find_element(By.TAG_NAME, 'a').get_attribute('href')
                vidList.append(link)
            if len(vidList) == 0:
                print("Something went wrong")
                self.driver.refresh()
                return self.scroll(xpath)
            print("Count of links: ", len(vidList))
            for vid in vidList:
                check_vid = self.check_link_crawled(vid)
                vidList.remove(vid)
        return vidList

    def get_link_list(self, key) -> list:
        print('-------> GET LINK LIST <-------')
        vidList = []
        # with open()
        # keyword_dict, option = self.parse_keyword()
        if option == "search_post":
            self.driver.get(self.config.search_post_tiktok + key)
            # time.sleep(1)
            # captcha.check_captcha(self.driver)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_SEARCH)
        if option == "search_post_android":
            driver_appium = run_appium(key)
            post = 0
            link = None
            while post <= 3:
                share = driver_appium.find_element(
                    "id", "com.ss.android.ugc.trill:id/ka3")
                share.click()
                copy_link = driver_appium.find_element(
                    "xpath", '//android.widget.Button[@content-desc="Sao chép Liên kết"]/android.widget.ImageView')
                copy_link.click()
                link_old = link
                link = clipboard.paste()
                # while link_old == link:
                #     time.sleep(1)
                time.sleep(5)
                self.driver.get(link)
                vid = self.driver.find_element(
                    By.XPATH, '//meta[@property="og:url"]').get_attribute("content")
                vidList.append(vid)
                # link_list.append(link)
                # with open("link_list_android.txt", "a") as f:
                #     f.write(f"{link}\n")
                perform_swipe(driver_appium)
                post += 1
                # with open("link_list_android.txt", "r") as f:
                #     vidList = [line.strip() for line in f.readlines()]
        elif option == "search_user": 
            self.driver.get(key)
            time.sleep(5)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_PAGE)
        elif option == "tag":
            self.driver.get(self.config.hashtag_tiktok + key)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        elif option == "explore":
            self.driver.get(self.config.explore_tiktok)
            div = self.driver.find_elements(
                By.XPATH, '//*[@id="main-content-explore_page"]/div/div[1]/div[1]/div')
            for d in div:
                if d.text == self.config.explore_option:
                    d.click()
                    break
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        return vidList
