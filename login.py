from seleniumwire import webdriver
import time
from selenium.webdriver.common.by import By
import json
import config
from account import Account

options = webdriver.ChromeOptions()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("--start-maximized")
options.add_argument('--disable-infobars')
options.add_argument('--disable-notifications')
options.add_argument('--disable-popup-blocking')
options.add_argument('--disable-save-password-bubble')
options.add_argument('--disable-translate')
options.add_argument('--disable-web-security')
options.add_argument('--disable-extensions')


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
        account = Account(data[self.username]["username"], data[self.username]
                          ["password"], data[self.username]["cookies"])
        return account

    def loginTiktokwithPass(self):
        login = self.driver.find_element(
            By.XPATH, '//*[@id="header-login-button"]')
        login.click()
        time.sleep(3)
        log_email = self.driver.find_element(
            By.XPATH, '//*[@id="loginContainer"]/div/div/a[2]/div')
        log_email.click()
        time.sleep(5)
        log = self.driver.find_element(
            By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[1]/a')
        log.click()
        time.sleep(3)
        user = self.driver.find_element(
            By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[1]/input')
        user.send_keys(self.account.username)
        password = self.driver.find_element(
            By.XPATH, '//*[@id="loginContainer"]/div[2]/form/div[2]/div/input')
        password.send_keys(self.account.password)
        button = self.driver.find_element(
            By.XPATH, '//*[@id="loginContainer"]/div[2]/form/button')
        button.click()

    def save_cookie(self):
        self.loginTiktokwithPass
        cookies_list = self.driver.get_cookies()
        cookies_dict = {}

        for cookie in cookies_list:
            cookies_dict[cookie['name']] = cookie['value']

        # print("cookies_dict", cookies_dict)
        cookies = "Cookie: fr=" + cookies_dict.get('fr') + "; sb=" + cookies_dict.get('sb') + "; datr=" + cookies_dict.get('datr') + "; wd=" + cookies_dict.get(
            'wd') + "; c_user=" + cookies_dict.get('c_user') + "; xs=" + cookies_dict.get('xs')

        with open('cookies2.json', 'r') as f:
            data = json.load(f)
        data[self.username] = cookies
        with open('cookies2.json', 'w') as f:
            json.dump(data, f)
        self.driver.close()
        return cookies

    def loginTiktokwithCookie(self):
        # with open('cookies2.json', 'r') as f:
        #     data = json.load(f)
        cookie = self.account.cookies
        # script = 'javascript:void(function(){ function setCookie(t) { var list = t.split("; "); console.log(list); for (var i = list.length - 1; i >= 0; i--) { var cname = list[i].split("=")[0]; var cvalue = list[i].split("=")[1]; var d = new Date(); d.setTime(d.getTime() + (7*24*60*60*1000)); var expires = ";domain=.facebook.com;expires="+ d.toUTCString(); document.cookie = cname + "=" + cvalue + "; " + expires; } } function hex2a(hex) { var str = ""; for (var i = 0; i < hex.length; i += 2) { var v = parseInt(hex.substr(i, 2), 16); if (v) str += String.fromCharCode(v); } return str; } setCookie("' + cookie + '"); location.href = "https://facebook.com"; })();'
        script = 'javascript:void(function(){ function setCookie(t) { var list = t.split("; "); console.log(list); for (var i = list.length - 1; i >= 0; i--) { var cname = list[i].split("=")[0]; var cvalue = list[i].split("=")[1]; var d = new Date(); d.setTime(d.getTime() + (7*24*60*60*1000)); var expires = ";domain=.tiktok.com;expires="+ d.toUTCString(); document.cookie = cname + "=" + cvalue + "; " + expires; } } function hex2a(hex) { var str = ""; for (var i = 0; i < hex.length; i += 2) { var v = parseInt(hex.substr(i, 2), 16); if (v) str += String.fromCharCode(v); } return str; } setCookie("' + cookie + '"); location.href = "https://tiktok.com"; })();'
        self.driver.execute_script(script)


# ### MAIN ###
# def main():
#     url = "https://www.tiktok.com/"
#     # path = config.account_path
#     username = ["babysunny2906","xinhxinh29"]
#     # with open (path, "r") as f:
#     #     data = f.read()
#     #     account = json.loads(data)
#     # acc = Account(account[username[0]]["username"], account[username[0]]["password"], account[username[0]]["cookies"])
#     driver = webdriver.Chrome(executable_path ='chromedriver.exe',options=options)
#     driver.get(url)
#     time.sleep(3)
#     ttLogin = TiktokLogin(driver, username = username[1])
#     ttLogin.loginTiktokwithCookie()
#     time.sleep(10)

# ### EXECUTE ###
# main()
