import threading
import logging
import time
import pandas as pd
import queue
import searcher_new as searcher
import math
import data.config as config
from PySimpleGUI import PopupGetFile


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


def main(work: [queue.SimpleQueue, list], break_after_first: bool, thread_col: int, doubling: bool, file):
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
            frame, not_found = data.pop()
            for appended, not_found_appended in data:
                frame = frame.append(appended)
                not_found = not_found.append(not_found_appended)
            frame.to_excel('result.xlsx')
            not_found.to_excel('not_found.xlsx')


def search(work, proxy):
    searcher_bot = searcher.Searcher(proxy=proxy)
    return searcher_bot.start(work, )


def search_queue(work, proxy, break_after_first, data):
    while not work.empty():
        if update_webdriver_lock.locked():
            time.sleep(1)
            continue
        else:
            searcher_bot = searcher.Searcher()
            result, not_found = searcher_bot.start_queue(work, break_after_first)
            with lock:
                data.append((result, not_found))


def function_to_thread(func, args, data):
    data.append(func(*args))


if __name__ == '__main__':
    try:
        file = PopupGetFile('Пожалуйста, укажите файл эксель с артикулами для поиска.')
        fr = pd.read_excel(file, index_col=0)
        doubling = False
        break_after_first = True  # if Sg.PopupYesNo('Прерываемся после второго найденного?') == 'Yes' else False
        thread_col = 1
        queue = queue.SimpleQueue()
        [queue.put((str(c).replace('.0', ''), '')) for c in fr.index if pd.notna(c)]
        main(queue, break_after_first, thread_col, doubling, file)
    except Exception as ex:
        logging.exception(ex, stack_info=True)

