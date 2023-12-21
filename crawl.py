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
from unidecode import unidecode
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
producer = KafkaProducer(bootstrap_servers=["192.168.143.54:9092"])




### OTHER ###
with open(config.config_path, "r", encoding='latin-1') as f:
    data = f.read()
    data = json.loads(data)
    option = data["mode"]["name"]
    listArgument = data["listArgument"]

chrome_options = webdriver.ChromeOptions()
for item in listArgument:
        chrome_options.add_argument(item)


class CrawlManage(object):
    XPATH_VIDEO_SEARCH = '//*[contains(@class, "DivItemContainerForSearch")]'
    XPATH_VIDEO_OTHER = '//*[contains(@class, "DivItemContainerV2")]'
    # XPATH_VIDEO_OTHER = '//*[@class="tiktok-x6y88p-DivItemContainerV2 e19c29qe9"]'
    XPATH_VIDEO_USER = '//*[@data-e2e="user-post-item-desc"]'

    def __init__(self, driver = webdriver.Chrome(options=chrome_options) , config=config) -> None:
        self.driver = driver
        self.config = config
        self.actions = ActionChains(self.driver)
        self.link = None
        self.comments = []
        self.reply = []

    def parse_keyword(self, option, page) -> List[str]:
        keyword_list: List[str] = []
        with open(self.config.config_path, "r", encoding='utf-8') as f:
            data = f.read()
            keyword_list_raw = json.loads(data)
            if option == "search_user":
                keyword_list = keyword_list_raw["mode"][f"list_page{page}"]
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
            post_extractor: PostTikTokExtractor = PostTikTokExtractor(
                driver=self.driver, link=link, source_id=source_id)
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
            
            if option != "update_post":
            # update post to data_crawled and write post to result
                update_json_file(file_path="data_crawled.json", new_link=link)
            write_post_to_file(post=post)
            
            # crawl cmt and push kafka
            if posts != [] :
                self.scroll_comment()
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
        format_str = "%m/%d/%Y %H:%M:%S"
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
        today_start = datetime(now.year, now.month, now.day, 0, 0, 0)
        yesterday_start = yesterday_start.strftime(format_str)
        today_start = today_start.strftime(format_str)
        link1 = get_link_es(type_list=['tiktok video'], gte = yesterday_start, lte=today_start)
        gte7 = now - timedelta(days=7)
        lte7 = now - timedelta(days=6)
        gte7 = gte7.replace(hour=0, minute=0, second=0, microsecond=0)
        lte7 = lte7.replace(hour=0, minute=0, second=0, microsecond=0)
        gte7_str = gte7.strftime(format_str)
        lte7_str = lte7.strftime(format_str)
        link7 = get_link_es(type_list=['tiktok video'], gte = gte7_str, lte=lte7_str)
        for link in link7:
            link1.append(link)
        return link1
    

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
    
    def run(self, page):
        count = 0
        self.driver.get("https://www.tiktok.com/")
        time.sleep(2)
        captcha.check_captcha(self.driver)
        ttLogin = TiktokLogin(self.driver, username = "xinhxinh29")
        ttLogin.loginTiktokwithCookie()
        # self.check_login_div()
        print("Start crawl")
        keywords=[]
        # time.sleep(3)
        if option == "update_post":
            schedule.every().day.at("11:25").do(self.update_post)
            while True:
                schedule.run_pending()
                time.sleep(1)
        else:
            keywords = self.parse_keyword(option, page)
        for keyword in keywords:
            link_list = self.get_link_list(keyword)
            for link in link_list:
                start = time.time()
                self.crawl_post(link)
                end = time.time()
                print(f"Time for video {link} is {end - start}")
        time.sleep(30*60)
        return self.run("")
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
    
    def check_link_crawled(self,data_crawled, link):
        id_check = {}
        page_name = link.split('@')[1].split('/')[0]
        video_id = link.split('/')[-1]
        if page_name in data_crawled:
            list_id = data_crawled[page_name]
            if video_id in list_id:
                return True
            else:
                return False
        else:
            return False
        

    def scroll(self, xpath):
        vidList = []
        # time.sleep(3)
        with open('data_crawled.json', 'r') as file:
            data_crawled = json.load(file)
        
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
                
                for vid in vid_list_elem:
                    i = 0
                    link = vid.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    if link in vidList:
                        continue
                    check = self.check_link_crawled(data_crawled,link)
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
                check_vid = self.check_link_crawled(data_crawled, vid)
                vidList.remove(vid)
        return vidList

    def get_link_list(self, keyword) -> list:
        print('-------> GET LINK LIST <-------')
        vidList = []
        # with open()
        # keyword_dict, option = self.parse_keyword()
        if option == "search_post":
            self.driver.get(self.config.search_post_tiktok + keyword)
            # time.sleep(1)
            # captcha.check_captcha(self.driver)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_SEARCH)
        if option == "search_post_android":
            driver_appium = run_appium(keyword)
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
            self.driver.get(keyword)
            # captcha.check_captcha(self.driver)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_USER)
        elif option == "tag":
            self.driver.get(self.config.hashtag_tiktok + keyword)
            # captcha.check_captcha(self.driver)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        elif option == "explore":
            self.driver.get(self.config.explore_tiktok)
            # captcha.check_captcha(self.driver)
            div = self.driver.find_elements(
                By.XPATH, '//*[@id="main-content-explore_page"]/div/div[1]/div[1]/div')
            for d in div:
                if d.text == self.config.explore_option:
                    d.click()
                    break
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        return vidList
