import queue
import threading
from crawl_post import CrawlManage
import config
from seleniumwire import webdriver as webdriver
import json
import captcha
from selenium.webdriver.common.by import By
import time
import concurrent.futures
from login import TiktokLogin

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--mute-audio')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument('--disable-notifications')
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-popup-blocking')
chrome_options.add_argument('--disable-save-password-bubble')
chrome_options.add_argument('--disable-translate')
chrome_options.add_argument('--disable-web-security')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--ignore_ssl")
# chrome_options.add_argument("--headless") 
# chrome_options.add_argument(f'--proxy-server={config.proxy}')

### MULTI ###

NUM_LINK_THREADS = 1
NUM_PROCESSING_THREADS = 1
lock = threading.Lock()
drivers = []
proxy_list=config.proxy

### XPATH ###
XPATH_VIDEO_SEARCH = '//*[contains(@class, "DivItemContainerForSearch")]'
XPATH_VIDEO_OTHER = '//*[contains(@class, "DivItemContainerV2")]'
# XPATH_VIDEO_OTHER = '//*[@class="tiktok-x6y88p-DivItemContainerV2 e19c29qe9"]'
XPATH_VIDEO_USER = '//*[@data-e2e="user-post-item-desc"]'

### OTHER ###
seleniumwire_options = {
    'verify_ssl': True,
    'disable_capture': False,
    'request_storage': 'memory',
    'request_storage_max_size': 0
}

### DEFINE ###
def get_config():
    with open(config.config_path, "r", encoding='latin-1') as f:
        data = f.read()
        data = json.loads(data)
    option = data["mode"]["name"]
    if option == "search_post":
        key_list = data["mode"]["keyword"]
    elif option == "search_post_android":
        pass
    elif option == "search_user": 
        key_list = data["mode"]["list_page"]
    elif option == "tag":
        key_list = data["mode"]["keyword"]
    elif option == "explore":
        pass
    return option, key_list
        
# def link_thread():
#     while True:
#         with lock:
#             link = link_queue.get()
#         crawl_link(driver, link)
#         with lock:
#             link_queue.task_done()

def get_driver():
    for i in range(NUM_PROCESSING_THREADS):
        proxy = proxy_list[i]
        driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=seleniumwire_options)
        drivers.append(driver)
        driver.get("https://www.tiktok.com/")
        login = TiktokLogin(driver=driver, username="xinhxinh29")
        login.loginTiktokwithCookie()
        time.sleep(2)
    return drivers
    

def crawl_link(driver,link_que):
    try:
        link = link_que.get(block=False) 
    except queue.Empty: 
        print("NOne")
    time.sleep(2)
    crawl = CrawlManage(driver)
    crawl.crawl_post(link)

    
def scroll(driver, option, xpath):
    link_list = []
    # time.sleep(3)
    try:
        captcha.check_captcha(driver)
    except:
        pass
    with open('dataCrawled.txt', 'r') as f:
        data_crawled = f.read()
    count = 1
    vid_list_elem = []
    if option == "search_user":
        try:
                no_post = driver.find_element(By.XPATH, '//*[@class="tiktok-1ovqurc-PTitle emuynwa1"]').text()
                print("No post")
        except:
                no_post =""
        while(len(vid_list_elem) != count and no_post != "No content"):
            count = len(vid_list_elem)
            try:
                    vid_list_elem = driver.find_elements(By.XPATH, xpath)
            except:
                    vid_list_elem = driver.find_elements(By.XPATH, xpath)
            for vid in vid_list_elem:
                link = vid.find_element(By.TAG_NAME, 'a').get_attribute('href')
                if link in data_crawled:
                    continue
                elif link in link_list:
                    continue
                else:
                    link_list.append(link)
            driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
    else:
        # time.sleep(3)
            while(len(vid_list_elem) != count):
                count = len(vid_list_elem)
                try:
                    vid_list_elem = driver.find_elements(By.XPATH, xpath)
                except:
                    vid_list_elem = driver.find_elements(By.XPATH, xpath)
                    # print("len vid: ", len(vid_list_elem))
                    driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
            for vid in vid_list_elem:
                link = vid.find_element(By.TAG_NAME, '.a').get_attribute('href')
                link_list.append(link)
            if len(link_list) == 0:
                print("Something went wrong")
                driver.refresh()
                return scroll(xpath)
            print("Count of links: ", len(link_list))
            for vid in link_list:
                if vid in data_crawled:
                    link_list.remove(vid)
    return link_list
# Luồng để đưa các liên kết vào hàng đợi
def get_link_list(link_queue):
    print('-------> GET LINK LIST <-------')
    # proxy_index = link_queue.qsize() % len(proxy_list)  # Chọn proxy dựa trên số thứ tự của liên kết
    # proxy = proxy_list[proxy_index]
    # chrome_options.add_argument(f"--proxy-server={proxy}")
    driver = webdriver.Chrome(options=chrome_options, seleniumwire_options = seleniumwire_options)
#     driver.scopes = [
#     'comment'
# ]
    drivers.append(driver)
    driver.get("https://www.tiktok.com/")
    login = TiktokLogin(driver=driver, username="xinhxinh29")
    login.loginTiktokwithCookie()
    time.sleep(2)
    link_list = []
    option, key_list = get_config()
    for key in key_list:
        link_list = []
        if option == "search_post":
            driver.get(config.search_post_tiktok + key)
            time.sleep(3)
                # time.sleep(1)
                # captcha.check_captcha(self.driver)
            link_list = scroll(driver, option, XPATH_VIDEO_SEARCH )
        if option == "search_post_android":
                pass
        elif option == "search_user": 
            driver.get(key)
            time.sleep(3)
            link_list = scroll(driver,option, XPATH_VIDEO_USER)
        elif option == "tag":
            driver.get(config.hashtag_tiktok + key)
            link_list = scroll(driver, option, XPATH_VIDEO_OTHER)
        elif option == "explore":
            driver.get(config.explore_tiktok)
                # captcha.check_captcha(self.driver)
            div = driver.find_elements(
                    By.XPATH, '//*[@id="main-content-explore_page"]/div/div[1]/div[1]/div')
            for d in div:
                if d.text == config.explore_option:
                    d.click()
                    break
            link_list = scroll(driver, option, XPATH_VIDEO_OTHER)
        for link in link_list:
            with lock:
                link_queue.put(link)
    
    # time.sleep(3*60*60)
    
def main():
    # Tạo và khởi chạy luồng lấy liên kết
    drivers = get_driver()
    link_queue = queue.Queue()
    get_link_list(link_queue)
    for driver in drivers:  
            print("-------Running parallel---------")
            t1 = threading.Thread(target=crawl_link, args=(driver, link_queue))
            t1.start()
    # Tạo và khởi chạy các luồng xử lý liên kết
    # with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_PROCESSING_THREADS) as processing_executor:
    #     for _ in range(NUM_PROCESSING_THREADS):
    #         processing_executor.submit()

    # Chờ cho tất cả các liên kết được xử lý
    link_queue.join()

    # Đóng tất cả các trình duyệt Selenium
    for driver in drivers:
        driver.quit()

main()

