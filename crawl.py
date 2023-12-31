from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time
import json
import schedule
from seleniumwire.utils import decode as sw_decode
import time
import config as cf
import json
import pickle
from typing import List
from post_tiktok_etractor import PostTikTokExtractor, PostCommentExtractor, PostReplyExtractor
from utils.common_utils import CommonUtils
import captcha
from kafka import KafkaProducer
from process_data import *
from login import TiktokLogin
from es import *
from datetime import datetime, timedelta
import clipboard
from get_link_from_android import *
from selenium.common.exceptions import TimeoutException
import requests
import os
producer = KafkaProducer(bootstrap_servers=["172.168.200.202:9092"])
api_address='http://172.168.200.200:8000'



class CrawlManage(object):
    XPATH_VIDEO_SEARCH = '//*[contains(@class, "DivItemContainerForSearch")]'
    XPATH_VIDEO_OTHER = '//*[contains(@class, "DivItemContainerV2")]'
    # XPATH_VIDEO_OTHER = '//*[@class="tiktok-x6y88p-DivItemContainerV2 e19c29qe9"]'
    XPATH_VIDEO_PAGE = '//*[@data-e2e="user-post-item-desc"]'

    def __init__(self, config = None) -> None:
        self.config = config
        chrome_options = self.get_chrome_arg()
        chrome_service = Service("chromedriver.exe")
        self.driver = webdriver.Chrome(service=chrome_service,options=chrome_options,seleniumwire_options = self.seleniumwire_options())
        self.wait = WebDriverWait(self.driver, 10)
        self.mode = self.config["mode"]["name"]
        self.driver.set_page_load_timeout(200)
        self.link = None
        self.stop_event = None
        self.comments = []
        self.reply = []
        
    def seleniumwire_options(self):
        seleniumwire_options = {
                                    # 'disable_encoding': True ,
                                    # 'request_storage': 'memory',
                                    'request_storage_max_size': 100,
                                }
        return seleniumwire_options
        
    def get_chrome_arg(self):
        listArgument = self.config["listArgument"]
        proxy_config = self.config["account"]["proxy"]
        chrome_options = webdriver.ChromeOptions()
        for item in listArgument:
                chrome_options.add_argument(item)
        # chrome_options.add_argument(f'--proxy-server={proxy_config}')
        return chrome_options

    def parse_keyword(self) -> List[str]:
        keyword_list: List[str] = []
        
        if self.mode == "search_user":
            keyword_list = self.config["mode"][f"list_page"]
        elif self.mode == "search_post_android" or self.mode == "search_post":
                keyword_list = self.config["mode"]["keyword"]
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
            del comment_extractor
            comments.append(comment)
            write_post_to_file(post=comment)
            try: 
                if comment_dict["reply_comment"] is not None:
                    list_reply = comment_dict["reply_comment"]
                    for reply_dict in list_reply:
                        reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
                        reply = reply_extractor.extract()
                        del reply_extractor
                        comments.append(reply)
                        write_post_to_file(post=reply)
            except Exception as e:
                 print(f"Error to crawl comment with Exception {e}")
        for reply_dict in list_replies:
            reply_extractor: PostReplyExtractor = PostReplyExtractor(driver = self.driver, reply_dict = reply_dict) 
            reply = reply_extractor.extract()
            del reply_extractor
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
            del post_extractor
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
            if self.mode == "update_post":
                #sroll to crawl comment
                self.scroll_comment()
            else:
                # update post to data_crawled and write post to result
                page_name = link.split('@')[1].split('/')[0]
                video_id = link.split('/')[-1]
                # try:
                #     self.insert(table_name="tiktok_video",object_id=page_name, links=video_id) 
                # except:
                #     print("Cant insert link to api")
                update_file_crawled(page_name=page_name, video_id=video_id)
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
        if self.mode == "update_post":
            topic = "osint-posts-update"
        else:
            topic = "osint-posts-raw"
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
        range_date = self.config["mode"]["range_date"]
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
    def interceptor(self,request):
    # Block PNG, JPEG and GIF images
        if request.path.endswith(('.png', '.jpg', '.gif')):
            request.abort()


    
    def run(self):
        try:
            # self.driver.request_interceptor = self.interceptor
            self.driver.get("https://vt.tiktok.com/")
            # self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)
            captcha.check_captcha(self.driver)
            ttLogin = TiktokLogin(self.driver,account_config=self.config["account"])
            try:
                ttLogin.loginTiktokwithCookie()
            except Exception:
                try:
                    self.driver.refresh()
                    ttLogin.loginTiktokwithCookie()
                except:
                # print("Retry to login Exception {e}")
                    print("Try login with pass and save new cookie")
                    new_cookies = ttLogin.save_cookie()
                    if new_cookies:
                        print("Done save new cookie")
            del ttLogin
            print("Start crawl")
            key_search = []
            # time.sleep(3)
            if self.mode == "update_post":
                start_time_run = self.config["mode"]["start_time_run"]
                schedule.every().day.at(start_time_run).do(self.update_post)
                while True:
                    schedule.run_pending()
                    time.sleep(1)
            else:
                key_search = self.parse_keyword()
                for key in key_search:
                    if self.stop_event.is_set():
                        self.driver.quit()
                        break
                    else:
                        link_list = self.get_link_list(key)
                        for link in link_list:
                            if self.stop_event.is_set():
                                self.driver.quit()
                                break
                            start = time.time()
                            self.crawl_post(link)
                            end = time.time()
                            print(f"Time for video {link} is {end - start}")
            # del self.driver.request_interceptor
            print("Sleep 30ph")
            time.sleep(30*60)
            return self.run() 
        except TimeoutException:
            print("Time out, try again in 15mins")
            self.driver.quit()
            time.sleep(15*60)
            crawl = CrawlManage()
            return crawl.run()
        
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
        page_name = link.split('@')[1].split('/')[0]
        video_id = link.split('/')[-1]
        # try:
        #     data_crawled = self.get_links(table_name="tiktok_video", object_id= page_name)
        #     if data_crawled:
        #         if video_id in data_crawled["links"]:
        #             return True
        #         else:
        #             return False
        #     else:
        #         return False
        # except:
        # print("Cant check link in api")
        folder_path = "dataCrawled"
        # Kiểm tra xem file có tên trùng với page_name tồn tại trong thư mục hay không
        file_path = os.path.join(folder_path, f"{page_name}.txt")
        if not os.path.exists(file_path):
            return False
        # Đọc nội dung của file
        with open(file_path, "r") as file:
            content = file.read().splitlines()
        # Kiểm tra xem video_id có trùng với phần tử nào trong nội dung hay không
        if video_id in content:
            return True
        return False
            
        # return False

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
        if self.mode == "search_user":
            # time.sleep(5)
            try:
                no_post = self.driver.find_element(By.XPATH, '//*[@class="tiktok-1ovqurc-PTitle emuynwa1"]').text()
                print("No post")
            except:
                no_post =""
            while(len(vid_list_elem) != count and len(vid_list_elem) < cf.count_of_vid and no_post != "No content"):
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
            while(len(vid_list_elem) != count and len(vid_list_elem) < cf.count_of_vid):
                    
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
                time.sleep(10)
                return self.scroll(xpath)
            print("Count of links: ", len(vidList))
            for vid in vidList:
                check_vid = self.check_link_crawled(vid)
                if check_vid:
                    vidList.remove(vid)
        return vidList

    def get_link_list(self, key) -> list:
        print('-------> GET LINK LIST <-------')
        vidList = []
        # with open()
        # keyword_dict, option = self.parse_keyword()
        if self.mode == "search_post":
            self.driver.get(cf.search_post_tiktok + key)
            self.wait.until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
            # time.sleep(1)
            # captcha.check_captcha(self.driver)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_SEARCH)
        if self.mode == "search_post_android":
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
        elif self.mode == "search_user":
            self.driver.get(key)
            self.wait.until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
            time.sleep(5)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_PAGE)
        elif self.mode == "tag":
            self.driver.get(cf.hashtag_tiktok + key)
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        elif self.mode == "explore":
            self.driver.get(cf.explore_tiktok)
            div = self.driver.find_elements(
                By.XPATH, '//*[@id="main-content-explore_page"]/div/div[1]/div[1]/div')
            for d in div:
                if d.text == cf.explore_option:
                    d.click()
                    break
            vidList = self.scroll(xpath=self.XPATH_VIDEO_OTHER)
        return vidList
