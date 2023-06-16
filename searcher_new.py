import copy
import queue
import random
import logging
import json
import requests
import pandas as pd
import time
import os
from hyper.contrib import HTTP20Adapter

logging.basicConfig(filename='SEND ME TO ADMIN.log',
                    format='%s\nSearcher_new :: [{asctime}]\n{message}\n' % ('_'*83),
                    style='{')


def list_in_list(list_1, list_2):
    list_1 = [c.lower() for c in list_1]
    list_2 = [c.lower() for c in list_2]
    for block in list_1:
        if block in list_2 or block + ',' in list_2:
            continue
        else:
            return False
    return True


def standartize(text):
    res = text.replace('(', '').\
        replace(')', ''). \
        replace(' ', ''). \
        lower(). \
        replace(',', ''). \
        replace('.', '')
    return res


class Searcher:
    def __init__(self, debug_logger: logging.Logger = None):
        if not debug_logger:
            debug = logging.getLogger(__file__)
            debug.setLevel(logging.DEBUG)
            if 'debug.log' in os.listdir():
                with open('debug.log', 'w') as debug_file:
                    debug_file.write('')
            debug_handler = logging.FileHandler('debug.log')
            debug_handler.setLevel(logging.DEBUG)
            debug_formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
            debug_handler.setFormatter(debug_formatter)
            debug.addHandler(debug_handler)
            debug.debug('Настройка debug завершена')
            self.debug = debug
        else:
            self.debug = debug_logger
        self.session = requests.session()
        self.session.mount('https://', HTTP20Adapter())
        self.session.headers['Accept-Encoding'] = 'gzip, deflate'
        self.session.headers['User-Agent'] = 'ozonapp_android'
        self.session.headers['Accept'] = 'application/json; charset=utf-8'
        # self.session.headers['X-O3-Sample-Trace'] = 'false'
        # self.session.headers['X-O3-App-Name'] = 'ozonapp_android'

    def debug_print(self, message):
        self.debug.debug(message)
        print(message)

    def start_queue(self, work: queue.Queue, search_in_description: bool):
        errors = 0
        done = 0
        result = None
        not_found = pd.DataFrame(columns=['offer',
                                          'result']).set_index('offer')
        while not work.empty():
            offer, brand = work.get()
            offer = str(offer)
            self.debug_print(f'Начали {brand} {offer}')
            try:
                wait = random.randint(3, 7)
                self.debug_print(f'Ожидание - {wait} секунд')
                time.sleep(wait)
                search_result, data = self.search(f'{offer} {brand}'.strip())
                self.debug_print(f'Данные получены - {search_result}')
                if search_result:
                    links = self.find_correct_links(offer, data, search_in_description)
                    if not links.empty:
                        if result is None:
                            result = links
                        else:
                            result = pd.concat([result, links])
                    else:
                        not_found.loc[offer] = 'НЕ НАЙДЕНО'
                else:
                    not_found.loc[offer] = 'НЕ НАЙДЕНО'
                done += 1
            except Exception as Ex:
                errors += 1
                self.debug_print(Ex)
                result.loc[offer] = f'Произошла ошибка поиска, подробности в логе. Время: {time.asctime()}'
                if errors >= 10:
                    return result  # , self.work_list
                time.sleep(10 * errors)
        return result, not_found

    def search(self, text):
        try:
            url = f'https://api.ozon.ru/composer-api.bx/page/json/v2?url=/search/?text={text}&sorting=rating&from_global=true&anchor=false'
            resp = self.session.get(
                url,
            )
            if resp.status_code == 302:
                errors = 1
                while errors < 5:
                    self.debug_print('Словили статус ответа 302, пробуем повторить через 2 секунды')
                    time.sleep(2)
                    resp = self.session.get(
                        url,
                    )
                    if resp.status_code == 200:
                        break
                    errors += 1
                if resp.status_code != 200:
                    return False, 'Ошибка запроса данных о номенклатурах'
            try:
                data = resp.json()
            except Exception as ex:
                self.debug.exception(ex)
            key = [c for c in data["widgetStates"].keys() if ('searchResultsV2' in c) or ('searchResultsError' in c)][0]
            if 'error' in key.lower():
                return False, []
            else:
                offers = json.loads(data["widgetStates"][key])['items']
                return True, offers
        except Exception as ex:
            logging.exception(ex)
            self.debug.exception(ex)
            return False, 'Ошибка запроса данных о номенклатурах'

    def find_correct_links(self, text, offers, search_in_description):
        find_count = 1
        description = ''
        input_text = copy.deepcopy(text)
        correct_links = pd.DataFrame(
            columns=[
                'offer',
                'counter',
                'rating',
                'seller',
                'id',
                'name',
                'partnum(ozon)',
                'article(ozon)',
                'link',
                'photo',
                'other_photo',
                'finded',
            ]
        ).set_index(['offer', 'counter'])
        self.debug_print(f'Начали распознавание, количество карточек - {len(offers)}')
        for offer_counter, offer in enumerate(offers):
            try:
                finded = []
                offer_name = [
                    c for c in offer['mainState'] if ('id' in c.keys()) and (c['id'] == 'name')
                ] \
                    [0]['atom']['textAtom']['text'].replace('&#x2F;', '/').replace('&#34;', '"').replace('&#39;', "'")
                if find_count == 4:
                    self.debug_print(f'Найдено совпадений - {find_count - 1}, разрываем цикл. всего перебрано вариантов - {offer_counter}')
                    break
                if offer_name:
                    cuted_name = standartize(offer_name)
                    text = text.replace(' ', '')
                    if standartize(text) in cuted_name:
                        finded.append('наименование')
                    # TODO: Прописать сверки по партнам или артикулу + описанию товара
                    partnum = ''
                    article = ''
                    link = offer['action']['link']
                    link = link.replace(f'/{link.split("/")[-1]}', '') if not link[-1].isdigit() or len(link) > 140 else link
                    if search_in_description:
                        time_to_sleep = 3 + random.randint(0, 3)
                        self.debug_print(f'Начали сбор {offer_counter + 1} карточки. Ожидание - {time_to_sleep}c.')
                        time.sleep(time_to_sleep)
                        advanced_data = self.session.get(f'https://api.ozon.ru/composer-api.bx/page/json/v2?url={link}').json()
                        try:
                            description = json.loads(advanced_data['seo']['script'][0]['innerHTML'])['description']
                        except KeyError:
                            self.debug_print(f'Описание в проверяемой карточке не обнаружено {advanced_data.setdefault("incidentId", "")}')
                        if description:
                            print('Описание получено')
                        else:
                            pass
                            # print('Описание пустое')
                        if standartize(text) in standartize(description):
                            finded.append('описание')
                    if finded:
                        self.debug_print('Найдено совпадение')
                        seller = ''
                        try:
                            labels = \
                                [c['atom']['labelList'] for c in offer['mainState'] if
                                 c['atom']['type'] == 'labelList'][0][
                                    'items']
                            label = \
                            [c for c in labels if ('icon' in c.keys()) and (c['icon']['tintColor'] == 'ozRating')][
                                0]
                            rating = label['title']
                            self.debug_print(f'Finded rating {rating}')
                        except IndexError:
                            rating = 0
                        link = 'https://ozon.ru' + link
                        photos = [c['image']['link'] for c in offer['tileImage']['items']]
                        photo = photos[0]
                        if len(photos) > 1:
                            other_photo = ' '.join(photos[1:])
                        else:
                            other_photo = ''
                        sku = link.split('-')[-1].replace('https://ozon.ru/product/', '')
                        correct_links.loc[(input_text, find_count), :] = [
                            rating,
                            seller,
                            sku,
                            offer_name,
                            partnum,
                            article,
                            link,
                            photo,
                            other_photo,
                            ', '.join(finded).capitalize() + '.',
                        ]
                        find_count += 1
            except Exception as Ex:
                logging.exception(Ex)
                self.debug.exception(Ex)
                raise
        return correct_links.reset_index(drop=False).set_index('offer')