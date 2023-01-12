import os
import threading
import logging
import time
import pandas as pd
import queue
import selenium.common.exceptions
import searcher_new as searcher
import math
import data.config as config
from PySimpleGUI import PopupGetFile
import PySimpleGUI as Sg


logging.basicConfig(filename='SEND ME TO ADMIN.log',
                    format='%s\n[{asctime}]\n{message}\n' % ('_'*83),
                    style='{')
lock = threading.RLock()
update_webdriver_lock = threading.Lock()


def pop_list(list_to_pop, num):
    data = []
    for i in range(num):
        try:
            data.append(list_to_pop.pop())
        except:
            continue
    return data


def main(work: [queue.SimpleQueue, list], break_after_first: bool, thread_col: int, doubling: bool):
    proxies = config.proxies * 2 if doubling else config.proxies
    proxies = proxies[:thread_col] if (thread_col != 0) and (thread_col > 0) else proxies
    if type(work) is list:
        assert len(work) > 0, 'Ошибка очереди заданий'
        work_len = math.ceil(len(work)/len(proxies))
        data = []
        works = [
            threading.Thread(
                target=function_to_thread,
                args=(search, (pop_list(work, work_len), proxy), data)
            ) for proxy in proxies
        ]
        for w in works:
            w.start()
            time.sleep(1)
        for w in works:
            w.join()
        if data:
            frame = data.pop()
            for appended in data:
                frame = frame.append(appended)
            frame.to_excel('result.xlsx')
    else:
        assert not work.empty(), 'Ошибка очереди заданий'
        data = []
        works = [
            threading.Thread(
                target=search_queue,
                args=(work, proxy, break_after_first, data)
            ) for proxy in proxies
        ]
        wait = 5
        for w in works:
            w.start()
            if wait:
                time.sleep(wait)
            wait = 0
        for w in works:
            w.join()
        if not queue.empty():
            errors = 1
            while errors < 5 and (not queue.empty()):
                works = [
                    threading.Thread(
                        target=search_queue,
                        args=(work, proxy, break_after_first, data)
                    ) for proxy in proxies
                ]
                for w in works:
                    w.start()
                for w in works:
                    w.join()

        if data:
            frame = data.pop()
            for appended in data:
                frame = frame.append(appended)
            frame.to_excel('result.xlsx')


def search(work, proxy):
    searcher_bot = searcher.Searcher(proxy=proxy)
    return searcher_bot.start(work, )


def search_queue(work, proxy, break_after_first, data):
    while not work.empty():
        if update_webdriver_lock.locked():
            time.sleep(1)
            continue
        else:
            try:
                searcher_bot = searcher.Searcher(proxy=proxy)
            except selenium.common.exceptions.SessionNotCreatedException as Ex:
                update_webdriver_lock.acquire()
                if 'session not created: This version of ChromeDriver only supports' in Ex.msg:
                    cuted_ex = Ex.msg.split('\n')[1].split(' ')
                    version = ''
                    for word in cuted_ex:
                        if len(word.split('.')) == 4:
                            version = word
                            break
                    import zipfile
                    import requests
                    versions_frame = pd.read_xml('https://chromedriver.storage.googleapis.com/')
                    actual_versions = versions_frame.dropna(subset=['Key']).query(
                        f'Key.str.contains("{".".join(version.split(".")[:-1])}") & Key.str.contains("win32.zip")')
                    last_version_address = actual_versions.iloc[-1]['Key']
                    resp = requests.get(f'https://chromedriver.storage.googleapis.com/{last_version_address}')
                    with open('temp', 'wb') as file:
                        file.write(resp.content)
                    zipp = zipfile.ZipFile('temp')
                    zipp.extractall()
                    zipp.close()
                    os.remove('temp')
                update_webdriver_lock.release()
                searcher_bot = searcher.Searcher(proxy=proxy)

            result = searcher_bot.start_queue(work, break_after_first)
            with lock:
                data.append(result)


def function_to_thread(func, args, data):
    data.append(func(*args))


if __name__ == '__main__':
    try:
        file = PopupGetFile('Пожалуйста, укажите файл эксель с артикулами для поиска.')
        fr = pd.read_excel(file, index_col=0)
        break_after_first = False
        doubling = False
        break_after_first = True if Sg.PopupYesNo('Прерываемся после второго найденного?') == 'Yes' else False
        thread_col = 0
        try:
            thread_col = int(Sg.PopupGetText("Пожалуйста, укажите требуемое количество потоков"))
            if thread_col > len(config.proxies):
                thread_col = 0
        except Exception as ex:
            logging.exception(ex, stack_info=True)
        queue = queue.SimpleQueue()
        if fr.empty:
            [queue.put((c, '')) for c in fr.index]
        else:
            [queue.put(c) for c in fr[fr.columns[0]].items()]
        main(queue, break_after_first, thread_col, doubling)
    except Exception as ex:
        logging.exception(ex, stack_info=True)

