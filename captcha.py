import cv2
import requests
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from seleniumwire  import webdriver
# from login import TiktokLogin
import circle


# options = webdriver.ChromeOptions()
# # options.add_argument('--disable-blink-features=AutomationControlled')
# options.add_argument("--start-maximized")
# # options.add_argument('--disable-infobars')
# options.add_argument('--disable-notifications')
# # options.add_argument('--disable-popup-blocking')
# # options.add_argument('--disable-save-password-bubble')
# # options.add_argument('--disable-translate')
# # options.add_argument('--disable-web-security')
# # options.add_argument('--disable-extensions')
# options.add_experimental_option("excludeSwitches", ["enable-automation"])
# # options.add_argument("--log-level=3")
# # # driver = webdriver.Chrome(options=options)


new_width = 340
new_height = 212
### CLASS ###
class Rotate:
    def __init__(self, driver):
        self.driver = driver
        
    def getLink(self):
        outer = self.driver.find_element(By.XPATH, '//*[@data-testid="whirl-outer-img"]').get_attribute(
                    "src")
        outer_image = outer
        inner = self.driver.find_element(By.XPATH, '//*[@data-testid="whirl-inner-img"]').get_attribute(
                    "src")
        inner_image = inner
        outer = 'outer.jpg'
        inner = 'inner.jpg'
        response = requests.get(outer_image)
        with open(outer, 'wb') as file:
            file.write(response.content)
        time.sleep(1)
        response = requests.get(inner_image)
        with open(inner, 'wb') as file:
            file.write(response.content)
        time.sleep(1)
        
    def rotateMatches(self):
        self.getLink()
        angle = circle.single_discern("inner.jpg","outer.jpg","result.png")
        px = (271 * angle) / 360
        return px
    
    def slider(self):
        c = 0
        while(c == 0):
            time.sleep(1.5)
            try:
                # secsdk-captcha-drag-icon sc-kEYyzF fiQtnm
                # button = self.driver.find_element(By.XPATH, '//*[@id="tiktok-verify-ele"]/div/div[2]/img[2]')
                button = self.driver.find_element(By.XPATH, '//*[@class="secsdk-captcha-drag-icon sc-kEYyzF fiQtnm"]')
                pixels = self.rotateMatches()
                print("Slider to solve rotate captcha")
                actions = ActionChains(self.driver)
                actions.move_to_element(button).perform()
                step = 25
                total = 0
                actions.click_and_hold(button).perform()
                while (total < pixels):
                    if (pixels - total) < step:
                        step = pixels - total
                    else:
                        pass
                    actions.move_by_offset(step, 0).perform()
                    actions.pause(0.1).perform()
                    total += step
                actions.pause(0.5).perform()
                actions.click().perform()
                
            except Exception as e:
                c = 1
                print("Solvered")
                break
             
    def rotateSolver(self):
        print("Solver captcha")
        return self.slider()
        
    def check_captcha(self):
        try:
            print("Check Captcha")
            self.driver.find_element(By.XPATH, '//*[@data-testid="whirl-outer-img"]')
            print("Solver captcha rortate")
            # driver.find_element(By.XPATH, '//*[@id="tiktok-verify-ele"]/div/div[1]/div[2]/div')
            return self.rotateSolver()
        except:
            print("No captcha")
            
class Puzzle:
    def __init__(self, driver):
        self.driver = driver
        
    def getLink(self):
        puzzle = self.driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]').get_attribute(
                    "src")
        puzzle_image = puzzle
        piece = self.driver.find_element(By.XPATH, '//*[@id="tiktok-verify-ele"]/div/div[2]/img[2]').get_attribute(
                    "src")
        piece_image = piece
        puzzle = 'captcha/puzzle.jpg'
        piece = 'captcha/piece.jpg'
        response = requests.get(puzzle_image)
        with open(puzzle, 'wb') as file:
            file.write(response.content)
        time.sleep(2)
        response = requests.get(piece_image)
        with open(piece, 'wb') as file:
            file.write(response.content)
        time.sleep(2)
        
    def puzzleMatches(self):
        self.getLink()
        img_rgb = cv2.imread('captcha/puzzle.jpg')
        img_rgb = cv2.resize(img_rgb, (new_width, new_height))
        img_gray = cv2.cvtColor(img_rgb, cv2.COLOR_BGR2GRAY)
        template = cv2.imread('captcha/piece.jpg',0)
        template = cv2.resize(template, (68, 68))
        res = cv2.matchTemplate(img_gray, template, cv2.TM_CCOEFF)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
        time.sleep(2)
        return int(min_loc[0])
    
    def slider(self):
        c = 0
        while(c == 0):
            time.sleep(2)
            try:
                # secsdk-captcha-drag-icon sc-kEYyzF fiQtnm
                # button = self.driver.find_element(By.XPATH, '//*[@id="tiktok-verify-ele"]/div/div[2]/img[2]')
                button = self.driver.find_element(By.XPATH, '//*[@class="secsdk-captcha-drag-icon sc-kEYyzF fiQtnm"]')
                pixels = self.puzzleMatches()
                print("Slider to solve puzzle captcha")
                actions = ActionChains(self.driver)
                actions.move_to_element(button).perform()
                step = 25
                total = 0
                actions.click_and_hold(button).perform()
                while (total < pixels):
                    if (pixels - total) < step:
                        step = pixels - total
                    else:
                        pass
                    actions.move_by_offset(step, 0).perform()
                    actions.pause(0.15).perform()
                    total += step
                actions.pause(0.5).perform()
                actions.click().perform()
                
            except Exception as e:
                c = 1
                print("Solvered")
                break
             
    def puzzleSolver(self):
        print("Solver captcha")
        return self.slider()
        
    def check_captcha(self):
        try:
            print("Check Captcha")
            self.driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]')
            print("Solver captcha puzzle")
            # driver.find_element(By.XPATH, '//*[@id="tiktok-verify-ele"]/div/div[1]/div[2]/div')
            return self.puzzleSolver()
        except:
            print("No captcha")
            
def check_captcha(driver):
    print("Check captcha")
    time.sleep(2)
    try:
        driver.find_element(By.XPATH, '//*[@id="captcha-verify-image"]')
        print("Solver captcha PUZZLE")
        return Puzzle(driver).puzzleSolver()
    except:
        try:
            driver.find_element(By.XPATH, '//*[@data-testid="whirl-outer-img"]')
            print("Solver captcha ROTATE")
            return Rotate(driver).rotateSolver()
        except:
            print("No captcha")
    
        
# def main():
#     driver = webdriver.Chrome(options=options)
#     #### GLOBAL INIT #####
#     url = 'https://www.tiktok.com/'
#     account = 'account.json'
#     # driver = webdriver.Chrome(executable_path ='chromedriver.exe',options=options)
#     driver.get(url)
#     time.sleep(3)
#     # ttLogin = TiktokLogin(driver, account)
#     # ttLogin.loginTiktokwithCookie()
#     search_key = 'marvel'
#     type_key = 'video'
#     search = f"https://www.tiktok.com/search/{type_key}?q={search_key}&t=1690885148972"
#     driver.get(search)
#     # driver.get('https://www.tiktok.com/@mavv.aep/video/7260147215178714369')
    
#     rotate = Rotate(driver)
#     rotate.rotateSolver()
# #     # puzzle = Puzzle(driver)  # Khởi tạo đối tượng Puzzle
# #     # puzzle.puzzleSolver()  # Gọi phương thức puzzleSolver()
# #     time.sleep(100)
    
    
    
# # ### EXECUTE ###
# main()

