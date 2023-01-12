import os
import subprocess
import zipfile

import requests
from selenium import webdriver
import winreg
import gdown
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
import selenium.common.exceptions as selenium_exceptions
from sys import platform

logging.basicConfig(
    format="\n\n%(asctime)s - %(name)s - %(levelname)s\n%(message)s",
    filename='main_log.log',
    level=logging.WARNING
)
logger = logging.getLogger(__name__)
abs_path = os.getcwd()


def logged_func(func):
    def logged(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logger.exception(ex)
            raise ex
    return logged


class Driver:
    def __init__(self, proxy='', wait=5):
        self.error_reloads = 0
        self.wait = wait
        self.proxy = proxy
        self.zip_path = ''
        self.driver_patched = False
        self.__check_driver()
        self.options = self.__get_options()
        self.driver = webdriver.Chrome(options=self.options)
        self._configure_headless()
        self.driver.implicitly_wait(self.wait)

    def find_element(self, by='', selector=''):
        return self.driver.find_element(by, selector)

    def find_elements(self, by='', selector=''):
        return self.driver.find_elements(by, selector)

    @logged_func
    def waited(self, css_selector: str, wait=10):
        try:
            wait = WebDriverWait(self.driver, timeout=wait)
            wait.until(
                visibility_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            return self.driver.find_element(By.CSS_SELECTOR, css_selector)
        except selenium_exceptions.TimeoutException:
            return False

    @logged_func
    def get(self, url):
        self.driver.get(url)
        try:
            err = self.driver.find_element(By.CSS_SELECTOR, 'div[class="core-msg spacer"]')
        except selenium_exceptions.NoSuchElementException:
            return self.driver
        logger.debug('Обнаружена затычка CLoudFlare. Иниициирую перезапуск драйвера.')
        print('Обнаружена затычка CLoudFlare. Иниициирую перезапуск драйвера.')
        if self.error_reloads < 20:
            self.error_reloads += 1
            # self.driver.close()
            self.driver.quit()
            self.driver = webdriver.Chrome(options=self.options)
            self.driver_patched = False
            self._configure_headless()
            # self.driver = uc.Chrome(options=self.__get_options())
            self.driver.implicitly_wait(self.wait)
            self.get(url)
        else:
            logger.exception('Превышение лимита перезапуска сессии.')
            raise EnvironmentError('Превышение лимита перезапуска сессии.')

    @logged_func
    def __get_options(self):
        chrome_options = webdriver.ChromeOptions()
        if self.proxy:
            data = []
            [data.extend(c.split(':')) for c in self.proxy.split('@')]
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
            pluginfile = f'{host}..{port}.zip'
            with zipfile.ZipFile(pluginfile, 'w') as zp:
                zp.writestr("manifest.json", manifest_json)
                zp.writestr("background.js", background_js)
            chrome_options.add_extension(pluginfile)
            self.zip_path = abs_path + '\\' + pluginfile
        # chrome_options.headless = True
        # chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--window-size=1920,1080")
        # chrome_options.add_argument("--start-maximized")
        # chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        # chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=3")
        return chrome_options

    def _configure_headless(self):
        orig_get = self.get
        logger.info("setting properties for headless")

        def get_wrapped(*args, **kwargs):
            # if self.driver.execute_script("return navigator.webdriver"):
            if not self.driver_patched:
                logger.info("patch navigator.webdriver")
                self.driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {
                        "source": """
    
                            Object.defineProperty(window, 'navigator', {
                                value: new Proxy(navigator, {
                                        has: (target, key) => (key === 'webdriver' ? false : key in target),
                                        get: (target, key) =>
                                                key === 'webdriver' ?
                                                false :
                                                typeof target[key] === 'function' ?
                                                target[key].bind(target) :
                                                target[key]
                                        })
                            });
    
                    """
                    },
                )

                logger.info("patch user-agent string")
                self.driver.execute_cdp_cmd(
                    "Network.setUserAgentOverride",
                    {
                        "userAgent": self.driver.execute_script(
                            "return navigator.userAgent"
                        ).replace("Headless", "")
                    },
                )
                self.driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {
                        "source": """
                            Object.defineProperty(navigator, 'maxTouchPoints', {
                                    get: () => 1
                            })"""
                    },
                )
                self.driver_patched = True
            return orig_get(*args, **kwargs)

        self.get = get_wrapped

    @logged_func
    def __check_driver(self):
        if platform == 'win32':
            if 'chromedriver.exe' not in os.listdir(abs_path):
                print('Драйвер не найден, запускаем загрузку актуальной версии')
                sys_chrome_version = self.__get_system_chrome_version()
                actual_driver_version = self.__actual_webdriver(sys_chrome_version)
                self.__update_driver(actual_driver_version)
            else:
                # pass
                sys_chrome_version = self.__get_system_chrome_version()
                actual_driver_version = self.__actual_webdriver(sys_chrome_version)
                cmd_out = subprocess.check_output('./chromedriver.exe -version').decode('utf8')
                inplace_driver_version = cmd_out.split(' ')[1]
                if inplace_driver_version == actual_driver_version:
                    print('Драйвер сверен и имеет актуальную версию')
                else:
                    print('Версии действующего драйвера и предлагаемого компанией Google различаются.\n'
                          'Запускаем обновление.')
                    # self.__update_driver(actual_driver_version)
        elif platform in ['linux', 'linux2']:
            pass

    @logged_func
    def __actual_webdriver(self, sys_chrome_version):
        if platform == 'win32':
            version_url = 'https://chromedriver.storage.googleapis.com/LATEST_RELEASE_' + \
                          '.'.join(sys_chrome_version.split('.')[:-1])
            actual_driver = requests.get(version_url).text
            return actual_driver
        elif platform in ['linux', 'linux2']:
            pass

    @logged_func
    def __update_driver(self, version):
        if platform == 'win32':
            download_url = f'https://chromedriver.storage.googleapis.com/' \
                           f'{version}/chromedriver_win32.zip'
            gdown.download(download_url, output='driver.zip', quiet=False)
            with zipfile.ZipFile(abs_path + '\\driver.zip') as zip_file:
                zip_file.extractall(abs_path + '\\')
            os.remove(abs_path + '\\driver.zip')
        elif platform in ['linux', 'linux2']:
            pass

    @staticmethod
    @logged_func
    def __get_system_chrome_version():
        if platform == 'win32':
            a_reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
            a_key = winreg.OpenKey(a_reg, r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall")
            version = ''
            for num in range(0, winreg.QueryInfoKey(a_key)[0]):
                sub_key_name = winreg.EnumKey(a_key, num)
                sub_key = winreg.OpenKey(a_key, sub_key_name)
                try:
                    disp_name = winreg.QueryValueEx(sub_key, 'DisplayName')[0]
                    if disp_name == 'Google Chrome':
                        version = winreg.QueryValueEx(sub_key, 'Version')[0]
                        break
                except Exception as ex:
                    pass
            if not version:
                raise FileNotFoundError('GoogleChrome в системе не обнаружен. Проверьте, что установили его.')
            return version
        elif platform in ['linux', 'linux2']:
            pass

    @logged_func
    def __del__(self):
        try:
            if self.zip_path:
                os.remove(self.zip_path)
            else:
                pass
        except FileNotFoundError:
            pass
        except Exception as ex:
            logger.exception(ex)
        if self.driver:
        #     self.driver.close()
            self.driver.quit()

