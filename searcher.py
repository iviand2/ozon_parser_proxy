import selenium.webdriver as webdriver
import selenium.webdriver.common.keys as keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import selenium.common.exceptions
import pandas as pd
import time
import os
import zipfile


def list_in_list(list_1, list_2):
    contains_result = True
    list_1 = [c.lower() for c in list_1]
    list_2 = [c.lower() for c in list_2]
    for block in list_1:
        if block in list_2 or block + ',' in list_2:
            continue
        else:
            contains_result = False
    return contains_result


def page_down(driver: webdriver.Chrome, count=1):
    counter = 0
    while counter < count:
        driver.find_element('css selector', 'body').send_keys(keys.Keys.PAGE_DOWN)
        time.sleep(0.5)
        counter += 1
    time.sleep(1)


class Searcher:
    def __init__(self, work: list, proxy=None):
        assert work, 'Список работы к поиску не может быть пустым'
        self.work_list = work
        if proxy is None:
            self.driver = webdriver.Chrome('chromedriver.exe')
            self.driver_for_carts = webdriver.Chrome('chromedriver.exe')
        else:
            self.driver = self.get_chromedriver(proxy)
            self.driver_for_carts = self.get_chromedriver(proxy)
            # self.driver.get('http://2ip.ru/')
            # time.sleep(10)
            # self.driver.quit()
            # return
        self.driver.implicitly_wait(10)
        self.driver.maximize_window()
        self.driver_for_carts.implicitly_wait(10)
        self.driver_for_carts.maximize_window()
        self.wait = WebDriverWait(self.driver, 30, poll_frequency=0.5)
        self.wait_for_carts = WebDriverWait(self.driver_for_carts, 30, poll_frequency=3)
        # self.start()

    @staticmethod
    def get_chromedriver(proxy: str = None):
        chrome_options = webdriver.ChromeOptions()

        path = os.path.dirname(os.path.abspath(__file__))

        if proxy:
            data = []
            [data.extend(c.split(':')) for c in proxy.split('@')]
            user = data[0]
            password = data[1]
            host = data[2]
            port = int(data[3])

            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

            background_js = """
            var config = {
                    mode: "fixed_servers",
                    rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                    }
                };
    
            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    
            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }
    
            chrome.webRequest.onAuthRequired.addListener(
                        callbackFn,
                        {urls: ["<all_urls>"]},
                        ['blocking']
            );
            """ % (host, port, user, password)
            pluginfile = 'proxy_auth_plugin.zip'
            with zipfile.ZipFile(pluginfile, 'w') as zp:
                zp.writestr("manifest.json", manifest_json)
                zp.writestr("background.js", background_js)
            chrome_options.add_extension(pluginfile)
        driver = webdriver.Chrome(
            'chromedriver',
            chrome_options=chrome_options)
        return driver

    def start(self):
        done = 0
        url = 'https://www.ozon.ru/'
        self.driver.get(url)
        result = pd.DataFrame(columns=['offer',
                                       'counter',
                                       'name',
                                       'link',
                                       'partnum(ozon)',
                                       'article(ozon)',
                                       'id',
                                       'finded']).set_index('offer')
        # bot = telebot.TeleBot(bot_string)
        try:
            for offer in self.work_list:
                if self.search(offer):
                    self.pending()
                    links = self.find_correct_links(offer)
                    if not links.empty:
                        result = pd.concat([result, links])
                    else:
                        result.loc[offer] = 'НЕ НАЙДЕНО'
                done += 1
            self.driver.quit()
            self.driver_for_carts.quit()
            return result
        except Exception as Ex:
            # try:
            #     # bot.send_message(orchester_config.admin_chat_id, Ex)
            # except:
            #     pass
            self.driver.quit()
            self.driver_for_carts.quit()
            if result.empty:
                return result#, self.work_list
            else:
                return result#, self.work_list[done - 1:]

    def pending(self):
        try:
            self.wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, 'div[class="busy-box busy-box-pending"]')))
        except selenium.common.exceptions.TimeoutException:
            self.refresh_page()

    def refresh_page(self):
        self.driver.refresh()
        time.sleep(5)
        self.pending()

    def search(self, text):
        try:
            search_box = self.driver.find_element_by_css_selector('input[placeholder="Искать на Ozon"]')
        except selenium.common.exceptions.NoSuchElementException:
            try_counter = 0
            while True and try_counter < 10:
                try:
                    self.driver.refresh()
                    time.sleep(10)
                    search_box = self.driver.find_element_by_css_selector('input[placeholder="Искать на Ozon"]')
                    break
                except: try_counter += 1
            if try_counter == 10:
                raise selenium.common.exceptions.NoSuchElementException('Счетчик ошибок 10, не смогли найти поле поиска')
        try:
            search_box.click()
        except selenium.common.exceptions.ElementClickInterceptedException:
            all_buttons = self.driver.find_elements_by_css_selector('button')
            for butt in all_buttons:
                if butt.text.lower() == 'подтвердить':
                    butt.click()
                    break
        search_box.send_keys(keys.Keys.CONTROL + 'a')
        search_box.send_keys(keys.Keys.DELETE)
        search_box.send_keys(text)
        search_box.send_keys(keys.Keys.ENTER)
        return True

    def find_correct_links(self, text):
        text = str(text)
        time.sleep(5)
        offers_list = self.driver.find_elements_by_css_selector(
            'div[data-widget="searchResultsV2"] > div > div')[:5]
        offers_list.extend(
            self.driver.find_elements_by_css_selector('div[data-widget="soldOutResultsV2"] > div > div')[:5])
        correct_links = pd.DataFrame(columns=['offer', 'counter', 'name', 'link', 'partnum(ozon)', 'article(ozon)', 'id', 'finded', 'seller']). \
            set_index(['offer', 'counter'])
        counter = 0
        for link in offers_list:
            try:
                one_link = link.find_elements_by_xpath('./div/a')[0]
            except IndexError:
                continue
            cuted_name = one_link.text.replace('(', '').replace(')', '')
            name = one_link.text
            text = text.replace(' ', '')
            try:
                self.driver_for_carts.get(one_link.get_attribute('href'))
            except Exception as Ex:
                # bot.send_message(orchester_config.admin_chat_id, Ex)
                continue
            time.sleep(5)
            partnum = ''
            article = ''
            page_down(self.driver_for_carts, 6)
            try:
                seller = self.driver_for_carts.find_element('css selector', 'div[data-widget="webCurrentSeller"]').\
                    find_elements('css selector', 'a')[1].text
            except:
                seller = 'Не определено'
            for span in self.driver_for_carts.find_elements_by_css_selector('span'):
                try:
                    if span.text in ['Партномер']:
                        partnum = span.find_element_by_xpath('../../dd').text.replace(' ', '')
                        if article:
                            break
                    elif span.text in ['Артикул']:
                        article = span.find_element_by_xpath('../../dd').text.replace(' ', '')
                        if partnum:
                            break
                except Exception as Ex:
                    # raise Ex
                    # bot.send_message(orchester_config.admin_chat_id, f'{Ex}\n Ссылка :: {driver_for_carts.current_url}')
                    continue
            if partnum.lower() == text.lower() \
                    or article.lower() == text.lower() \
                    or list_in_list(str(text).strip().split(' '), cuted_name):
                counter += 1
                correct_links.loc[(text, counter), :] = [
                    name,
                    one_link.get_attribute('href'),
                    partnum,
                    article,
                    one_link.get_attribute('href').split('/')[-2].split('-')[-1],
                    'Наименование ' if str(text) in cuted_name else '' + 'Партномер ' if partnum else '' + 'Артикул' if article else '',
                    seller
                    ]
        return correct_links.reset_index().set_index('offer')



