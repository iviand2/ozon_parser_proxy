import threading
import logging
import time
import pandas as pd
import queue
import searcher_new as searcher
import math
import data.config as config
from PySimpleGUI import PopupGetFile, PopupYesNo


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


def main(work: [queue.SimpleQueue, list], search_in_description: bool, thread_col: int, doubling: bool, file=''):
    if file:
        appended_frame = pd.read_excel(file, converters={0: str, 2: str})
        cols = appended_frame.columns
        appended_frame = appended_frame.rename(
            columns={cols[2]: 'offer',
                     cols[1]: 'brand',
                     cols[0]: 'code_1c',
                     cols[3]: 'name_start'})
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
    else:
        assert not work.empty(), 'Ошибка очереди заданий'
        data = []
        works = [
            threading.Thread(
                target=search_queue,
                args=(work, proxy, search_in_description, data)
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
                        args=(work, proxy, search_in_description, data)
                    ) for proxy in proxies
                ]
                for w in works:
                    w.start()
                for w in works:
                    w.join()
        if data:
            frame, not_found = data.pop()
            file_name = file.split('/')[-1]
            file_name = file_name.replace('.' + file_name.split('.')[-1], '')
            for appended, not_found_appended in data:
                frame = frame.append(appended)
                not_found = not_found.append(not_found_appended)
            # Прописать джоин appended frame к результатам поиска
            if frame is not None:
                if file:
                    appended_frame = appended_frame.set_index('offer')
                    frame = frame.join(appended_frame, rsuffix='исходное', how='left')
                    need_columns = ['brand', 'code_1c', 'name_start', 'counter', 'rating', 'seller', 'id', 'name', 'partnum(ozon)', 'article(ozon)', 'link', 'photo', 'other_photo', 'finded']
                    need_columns += [c for c in frame.columns if c not in need_columns]
                    frame = frame[need_columns]
                frame.to_excel(f'{file_name} {" (desc) " if search_in_description else ""}result.xlsx')
            not_found.to_excel(f'{file_name} {" (desc) " if search_in_description else ""}not_found.xlsx')
            input('Обработка завершена\n'
                  f'Длина стартового массива - {len(appended_frame)}\n'
                  f'Найдено артикулов - {len({c for c in frame.index})}\n'
                  f'Не найдено артикулов - {len({c for c in not_found.index})}\n'
                  'нажмите <Enter> для закрытия')


def search(work, proxy):
    searcher_bot = searcher.Searcher(proxy=proxy)
    return searcher_bot.start(work, )


def search_queue(work, proxy, search_in_description, data):
    while not work.empty():
        if update_webdriver_lock.locked():
            time.sleep(1)
            continue
        else:
            searcher_bot = searcher.Searcher()
            result, not_found = searcher_bot.start_queue(work, search_in_description)
            with lock:
                data.append((result, not_found))


def function_to_thread(func, args, data):
    data.append(func(*args))


if __name__ == '__main__':
    try:
        file = PopupGetFile('Пожалуйста, укажите файл эксель с артикулами для поиска.\n'
                            'Поряок столбцов: Код 1с, Бренд, Partnum')
        fr = pd.read_excel(file, converters={2: str})
        fr = fr.set_index(fr.columns[2])
        doubling = False
        search_in_description = True if PopupYesNo('Ищем в описании?\nПроцедура займет больше времени.') == 'Yes' else False
        thread_col = 1
        queue = queue.SimpleQueue()
        if fr.empty:
            [queue.put((c, '')) for c in fr.index]
        else:
            [queue.put(c) for c in fr[fr.columns[1]].items()]
        main(queue, search_in_description, thread_col, doubling, file)
    except Exception as ex:
        logging.exception(ex, stack_info=True)
        print(ex)
        input('Во время выполнения возникла ошибка нажмите <Enter> для закрытия')
