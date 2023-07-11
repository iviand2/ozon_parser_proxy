import threading
import logging
import time
import pandas as pd
import queue
import searcher_new as searcher
import math
import data.config as config
from PySimpleGUI import PopupGetFile, PopupYesNo, PopupGetFolder, PopupGetText
import PySimpleGUI as sg
from sys import exit


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


def main(work: [queue.SimpleQueue, list], search_in_description: bool, thread_col: int, doubling: bool, file='', save_directory: str = ''):
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
                # frame.to_excel(f'{file_name} {"(desc) " if search_in_description else ""}result.xlsx')
                frame.to_excel(get_file_name(file_name, 'result', save_directory, search_in_description, extension='.xlsx'))
            # not_found.to_excel(f'{file_name} {"(desc) " if search_in_description else ""}not_found.xlsx')
            not_found.to_excel(get_file_name(file_name, 'not_found', save_directory, search_in_description, extension='.xlsx'))
            input('Обработка завершена\n'
                  f'Длина стартового массива - {len(appended_frame)}\n'
                  f'Найдено артикулов - {len({c for c in frame.index})}\n'
                  f'Не найдено артикулов - {len({c for c in not_found.index})}\n'
                  'нажмите <Enter> для закрытия')


def work_in_parts(work: list, search_in_description: bool, save_directory: str, part_size: int, file: str = ''):
    if file:
        appended_frame = pd.read_excel(file, converters={0: str, 2: str})
        cols = appended_frame.columns
        appended_frame = appended_frame.rename(
            columns={cols[2]: 'offer',
                     cols[1]: 'brand',
                     cols[0]: 'code_1c',
                     cols[3]: 'name_start'})
        appended_frame = appended_frame.set_index('offer')
    proxies = config.proxies * 2 if doubling else config.proxies
    proxies = proxies[:thread_col] if (thread_col != 0) and (thread_col > 0) else proxies
    works_list = split_list(work, part_size)
    data = []
    result_found = 0
    result_not_found = 0
    for count, w in enumerate(works_list):
        w_queue = queue_from_list(w)
        works = [
            threading.Thread(
                target=search_queue,
                args=(w_queue, proxy, search_in_description, data))
            for proxy in proxies
        ]
        if not w_queue.empty():
            for worker in works:
                worker.start()
            for worker in works:
                worker.join()
            if data:
                frame, not_found = data.pop()
                file_name = file.split('/')[-1]
                file_name = file_name.replace('.' + file_name.split('.')[-1], '')
                for appended, not_found_appended in data:
                    frame = frame.append(appended)
                    not_found = not_found.append(not_found_appended)
                if frame is not None:
                    if file:
                        frame = frame.join(appended_frame, rsuffix='исходное', how='left')
                        need_columns = ['brand', 'code_1c', 'name_start', 'counter', 'rating', 'seller', 'id', 'name',
                                        'partnum(ozon)', 'article(ozon)', 'link', 'photo', 'other_photo', 'finded']
                        need_columns += [c for c in frame.columns if c not in need_columns]
                        frame = frame[need_columns]
                    frame.to_excel(get_file_name(file_name, 'result', save_directory, search_in_description, '.xlsx', count))
                not_found.to_excel(get_file_name(file_name, 'not_found', save_directory, search_in_description, '.xlsx', count))
                result_found += len({c for c in frame.index})
                result_not_found += len({c for c in not_found.index})
        print('-' * 40)
    input('Обработка завершена\n'
          f'Длина стартового массива - {len(appended_frame)}\n'
          f'Найдено артикулов - {result_found}\n'
          f'Не найдено артикулов - {result_not_found}\n'
          'нажмите <Enter> для закрытия')


def get_file_name(file_name: str, postfix: str, save_directory: str = '', search_in_description: bool = False, extension: str = '.xlsx', part_number: int = None):
    part_number = f'_{str(part_number)}' if part_number is not None else ''
    save_directory = save_directory + '/' if save_directory else ''
    return f'{save_directory}{file_name} {"(desc) " if search_in_description else ""}{postfix}{part_number}{extension}'


def split_list(list_to_split, num):
    return [list_to_split[i:i + num] for i in range(0, len(list_to_split), num)]


def queue_from_list(_list):
    _queue = queue.SimpleQueue()
    [_queue.put(i) for i in _list]
    return _queue


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
        layout = [
            [sg.Text('Пожалуйста, укажите путь к файлу Excel с артикулами для поиска.\nПорядок столбцов: Код 1с, Бренд, Partnum')],
            [sg.Text('Путь к файлу'), sg.Input(key='file'), sg.FileBrowse()],
            [sg.Text('Путь к папке для сохранения'), sg.Input(key='save_directory'), sg.FolderBrowse()],
            [sg.Checkbox('Искать в описании? Процедура займет больше времени', key='search_in_description')],
            [sg.Text('Размер части'), sg.Input(default_text='50', key='part_size')],
            [sg.Submit(), sg.Cancel()]
        ]
        window = sg.Window('Поиск артикулов', layout)
        event, values = window.read()
        window.close()
        if event == sg.WIN_CLOSED or event == 'Cancel':
            exit()
        file = values.setdefault('file', '')
        save_directory = values.setdefault('save_directory', '')
        search_in_description = values.setdefault('search_in_description', False)
        part_size = int(values.setdefault('part_size', '50'))

        # file = PopupGetFile('Пожалуйста, укажите файл эксель с артикулами для поиска.\n'
        #                     'Порядок столбцов: Код 1с, Бренд, Partnum')
        # save_directory = PopupGetFolder('Пожалуйста, укажите папку для сохранения')
        # part_size = int(PopupGetText('Введите размер части', default_text='50'))
        # search_in_description = True if PopupYesNo('Ищем в описании?\nПроцедура займет больше времени.') == 'Yes' else False
        fr = pd.read_excel(file, converters={2: str})
        fr = fr.set_index(fr.columns[2])
        doubling = False
        thread_col = 1
        if part_size:
            task_list = []
            if fr.empty:
                [task_list.append((c, '')) for c in fr.index]
            else:
                [task_list.append(c) for c in fr[fr.columns[1]].items()]
            work_in_parts(task_list, search_in_description, save_directory, part_size, file)
        else:
            task_queue = queue.SimpleQueue()
            if fr.empty:
                [task_queue.put((c, '')) for c in fr.index]
            else:
                [task_queue.put(c) for c in fr[fr.columns[1]].items()]
            main(task_queue, search_in_description, thread_col, doubling, file)
        # main(task_queue, search_in_description, thread_col, doubling, file)
    except Exception as ex:
        logging.exception(ex, stack_info=True)
        print(ex)
        input('Во время выполнения возникла ошибка нажмите <Enter> для закрытия')
