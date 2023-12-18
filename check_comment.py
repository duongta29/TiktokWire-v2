from es import get_link_es
from datetime import datetime, timedelta
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
import time
import json
from seleniumwire.utils import decode as sw_decode
import time
import config 
import json
import pickle
from selenium.webdriver.support import expected_conditions as EC
from post_tiktok_etractor import PostTikTokExtractor, PostCommentExtractor, PostReplyExtractor
from utils.common_utils import CommonUtils
import config
import captcha
from kafka import KafkaProducer
from selenium.webdriver.common.action_chains import ActionChains
# from bs4 import BeautifulSoup
from login import TiktokLogin
producer = KafkaProducer(bootstrap_servers=["10.11.101.129:9092"])

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--mute-audio')
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--incognito')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument('--disable-notifications')
chrome_options.add_argument('--disable-popup-blocking')
chrome_options.add_argument('--disable-save-password-bubble')
chrome_options.add_argument('--disable-translate')
chrome_options.add_argument('--disable-web-security')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument(f'--proxy-server={config.proxy}')

### OTHER ###
with open(config.config_path, "r", encoding='latin-1') as f:
    data = f.read()
    data = json.loads(data)
    option = data["mode"]["name"]

class CrawlManage(object, ):
    

    def __init__(self, driver = webdriver.Chrome(options=chrome_options) , config=config) -> None:
#         driver.scopes = [
#     '.*comment.*'
# ]
        self.driver = driver
        self.config = config
        self.actions = ActionChains(self.driver)
        self.link = None
        self.comments = []
        self.reply = []

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
            with open("result.txt", "a", encoding="utf-8") as file:
                file.write(f"{str(comment)}\n")
                if comment.is_valid:
                    file.write("🇧🇷" * 50 + "\n")
                else:
                    file.write("🎈" * 50 + "\n")
            try: 
                if comment_dict["reply_comment"] is not None:
                    list_reply = comment_dict["reply_comment"]
                    for reply_dict in list_reply:
                        reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
                        reply = reply_extractor.extract()
                        comments.append(comment)
                        with open("result.txt", "a", encoding="utf-8") as file:
                            file.write(f"{str(reply)}\n")
                            if reply.is_valid:
                                file.write("🇧🇷" * 50 + "\n")
                            else:
                                file.write("🎈" * 50 + "\n")  
            except Exception as e:
                 print(e)
        for reply_dict in list_replies:
            reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
            reply = reply_extractor.extract()
            comments.append(comment)
            with open("result.txt", "a", encoding="utf-8") as file:
                file.write(f"{str(reply)}\n")
                if reply.is_valid:
                    file.write("🇧🇷" * 50 + "\n")
                else:
                    file.write("🎈" * 50 + "\n")
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
                    # comments_section = self.driver.find_element(By.XPATH, '//*[@data-e2e="search-comment-container"]/div')
                    # actions.move_to_element(comments_section)
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
         
    def check_post(self, source_id):
        data = []
        try:
            infor_text = self.driver.find_element(
                By.XPATH, '//*[@id="__UNIVERSAL_DATA_FOR_REHYDRATION__"]').get_attribute('text')
            infor_text = json.loads(infor_text)
            infor_text = infor_text["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"]
        except:
            infor_text = self.driver.find_element(By.XPATH, '//*[@id="SIGI_STATE"]').get_attribute('text')
            infor_text = json.loads(infor_text)
            infor_text = infor_text["ItemModule"][source_id]
        like = infor_text["stats"]["diggCount"]
        love = infor_text["stats"]["collectCount"]
        comment = infor_text["stats"]["commentCount"]
        share = infor_text["stats"]["shareCount"]
        data = [like, love, comment, share]
        return data
        

    def crawl_post(self, list_inf):
        link, like, love, comment, share = list_inf[:5]
        self.driver.response_interceptor = self.interceptor_post
        segments = link.split("/")
        source_id = segments[-1]
        try:
            posts=[]
            self.comments =[]
            self.driver.get(link) 
            self.driver.implicitly_wait(5)
            pre_like, pre_love, pre_comment, pre_share = self.check_post(source_id)[:4]
            if pre_comment != comment:
                post_extractor: PostTikTokExtractor = PostTikTokExtractor(
                    driver=self.driver, link=link, source_id=source_id)
                # data[vid] = self.CrawlVideo(vid)
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
                with open('dataCrawled.txt', 'a') as f:
                    f.write(f"{link}\n")
                with open("result.txt", "a", encoding="utf-8") as file:
                    file.write(f"{str(post)}\n")
                    if post.is_valid:
                        file.write("🇧🇷" * 50 + "\n")
                    else:
                        file.write("🎈" * 50 + "\n")
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
                        print(e)
            else:
                if pre_like != like or pre_share != share or pre_love != love:
                    post_extractor: PostTikTokExtractor = PostTikTokExtractor(
                    driver=self.driver, link=link, source_id=source_id)
                # data[vid] = self.CrawlVideo(vid)
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
                with open('dataCrawled.txt', 'a') as f:
                    f.write(f"{link}\n")
                with open("result.txt", "a", encoding="utf-8") as file:
                    file.write(f"{str(post)}\n")
                    if post.is_valid:
                        file.write("🇧🇷" * 50 + "\n")
                    else:
                        file.write("🎈" * 50 + "\n")
                del self.driver.response_interceptor
                    # print("push kafka")
                try:
                        push_kafka = self.push_kafka(posts,comments)
                        if push_kafka == 1:
                            print("Done push kafka")
                except Exception as e:
                        print(e)
                    
        except Exception as e:
            print(e)
            captcha.check_captcha(self.driver)
            return self.crawl_post(link)
        
    def push_kafka(self, posts, comments):
        if len(posts) > 0:
            bytes_obj = pickle.dumps([ob.__dict__ for ob in posts])
            producer.send('osint-posts-update', bytes_obj)
            if len(comments) > 0:
                bytes_obj = pickle.dumps([ob.__dict__ for ob in comments])
                producer.send('osint-posts-update', bytes_obj)
            return 1
        else:
            return 0

    def run(self, page):
        count = 0
        self.driver.get("https://www.tiktok.com/")
        time.sleep(2)
        captcha.check_captcha(self.driver)
        ttLogin = TiktokLogin(self.driver, username = "xinhxinh29")
        ttLogin.loginTiktokwithCookie()
        print("Start crawl")
        link_list = self.extract_info_from_file("data_tiktok_video.txt")
        for list_inf in link_list:
            start = time.time()
            self.crawl_post(list_inf)
            end = time.time()
            print(f"Time for video {list_inf[0]} is {end - start}")
        time.sleep(30*60)
        return self.run("")
    

    def extract_info_from_file(self,file_path):
        current_time = datetime.now()
        gte = current_time - timedelta(days=7)
        lte = current_time
        gte_str = gte.strftime('%m/%d/%Y %H:%M:%S')
        lte_str = lte.strftime('%m/%d/%Y %H:%M:%S')
        get_link_es(type_list="tiktok video", gte = gte_str, lte= lte_str )
        with open(file_path, 'r') as file:
            data = file.read()
        items = data.split('\n')
        results = []
        for item in items:
            parts = item.split('|')
            link = parts[0]
            comment = parts[1]
            timestamp = parts[2]
            results.append([link, comment, timestamp])
        return results



