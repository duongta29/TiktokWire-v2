from seleniumwire import webdriver
import time
from selenium.webdriver.common.by import By
import json
import config
from account import Account
import captcha

# with open(config.config_path, "r", encoding='latin-1') as f:
#     data = f.read()
#     data = json.loads(data)
#     option = data["mode"]["name"]
#     listArgument = data["listArgument"]
# chrome_options = webdriver.ChromeOptions()
# for item in listArgument:
#         chrome_options.add_argument(item)
# chrome_options.add_argument(f'--proxy-server={config.proxy}')
### CLASS ###

class TiktokLogin:
    def __init__(self, driver, username):
        self.driver = driver
        self.username = username
        self.account = self.get_account()
        # self.user = account['user']
        # self.password = account['password']

    def get_account(self):
        with open(config.account_path, "r") as f:
            data = f.read()
            data = json.loads(data)
        account = Account(username=self.username,password=data[self.username]
                          ["password"], cookies= data[self.username]["cookies"])
        return account

    def loginTiktokwithPass(self):
        captcha.check_captcha(self.driver)
        login = self.driver.find_element(By.XPATH, '//*[@id="header-login-button"]')
        login.click()
        time.sleep(3)
        log_email = self.driver.find_element(By.XPATH, '//*[contains(@class, "DivLoginOptionContainer")]/div[2]')
        log_email.click()
        time.sleep(3)
        try:
            log = self.driver.find_element(By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[1]/a')
            log.click()
        except Exception as e:
            print(e)
        time.sleep(3)
        user = self.driver.find_element(By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[1]/input')
        user.send_keys(self.account.username)
        password = self.driver.find_element(By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[2]/div/input')
        password.send_keys(self.account.password)
        button = self.driver.find_element(By.XPATH, '//*[@id="loginContainer"]/div[2]/form/button')
        button.click()
        time.sleep(5)

    def save_cookie(self):
        self.loginTiktokwithPass()
        try:
            cookies_list = self.driver.get_cookies()
            cookies_dict = {}
            for cookie in cookies_list:
                cookies_dict[cookie['name']] = cookie['value']
            cookies = f"tt_csrf_token={cookies_dict.get('tt_csrf_token')}; tt_chain_token={cookies_dict.get('tt_chain_token')}; perf_feed_cache={cookies_dict.get('perf_feed_cache')}; tiktok_webapp_theme={cookies_dict.get('tiktok_webapp_theme')}; passport_fe_beating_status=false; s_v_web_id= {cookies_dict.get('s_v_web_id')}; passport_csrf_token_default={cookies_dict.get('passport_csrf_token_default')}; passport_csrf_token={cookies_dict.get('passport_csrf_token')}; multi_sids={cookies_dict.get('passport_csrf_token')}; cmpl_token={cookies_dict.get('cmpl_token')}; passport_auth_status={cookies_dict.get('passport_auth_status')}; passport_auth_status_ss={cookies_dict.get('passport_auth_status_ss')}; sid_guard={cookies_dict.get('sid_guard')}; uid_tt={cookies_dict.get('uid_tt')}; uid_tt_ss={cookies_dict.get('uid_tt_ss')}; sid_tt={cookies_dict.get('sid_tt')}; sessionid={cookies_dict.get('sessionid')}; sessionid_ss={cookies_dict.get('sessionid_ss')}; sid_ucp_v1={cookies_dict.get('sid_ucp_v1')}; ssid_ucp_v1={cookies_dict.get('ssid_ucp_v1')}; store-idc={cookies_dict.get('store-idc')}; store-country-code=vn; store-country-code-src=uid; tt-target-idc=alisg; tt-target-idc-sign={cookies_dict.get('tt-target-idc-sign')}; odin_tt={cookies_dict.get('odin_tt')}; ttwid={cookies_dict.get('ttwid')}; msToken={cookies_dict.get('msToken')}; passport_fe_beating_status=false"
            with open('db/account.json', 'r') as f:
                data = json.load(f)
            data[self.username]["cookies"] = cookies
            with open('db/account.json', 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(e)
            # self.driver.close()
        return cookies

    def loginTiktokwithCookie(self):
        cookie = self.account.cookies
        script = 'javascript:void(function(){ function setCookie(t) { var list = t.split("; "); console.log(list); for (var i = list.length - 1; i >= 0; i--) { var cname = list[i].split("=")[0]; var cvalue = list[i].split("=")[1]; var d = new Date(); d.setTime(d.getTime() + (7*24*60*60*1000)); var expires = ";domain=.tiktok.com;expires="+ d.toUTCString(); document.cookie = cname + "=" + cvalue + "; " + expires; } } function hex2a(hex) { var str = ""; for (var i = 0; i < hex.length; i += 2) { var v = parseInt(hex.substr(i, 2), 16); if (v) str += String.fromCharCode(v); } return str; } setCookie("' + cookie + '"); location.href = "https://tiktok.com"; })();'
        self.driver.execute_script(script)


# ### MAIN ###
# def main():
#     url = "https://www.tiktok.com/"
#     # path = config.account_path
#     username = "babysunny2906"
#     # with open (path, "r") as f:
#     #     data = f.read()
#     #     account = json.loads(data)
#     # acc = Account(account[username[0]]["username"], account[username[0]]["password"], account[username[0]]["cookies"])
#     driver = webdriver.Chrome(options=chrome_options)
#     driver.get(url)
#     time.sleep(3)
#     ttLogin = TiktokLogin(driver, username = username)
#     # ttLogin.loginTiktokwithPass()
#     ttLogin.loginTiktokwithCookie()
#     # cookies = ttLogin.save_cookie()
#     # print(cookies)
#     time.sleep(10)

# ### EXECUTE ###
# main()
