import copy
import queue
import random
import logging
import json
import requests
import pandas as pd
import time
import collections.abc
# hyper needs the four following aliases to be done manually.
collections.Iterable = collections.abc.Iterable
collections.Mapping = collections.abc.Mapping
collections.MutableSet = collections.abc.MutableSet
collections.MutableMapping = collections.abc.MutableMapping
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
    def __init__(self):
        self.session = requests.session()
        self.session.mount('https://', HTTP20Adapter())
        # self.session.proxies = {
        #     # 'http': 'localhost:8080',
        #     'https': '127.0.0.1:8080'
        # }


        self.session.headers['Accept-Encoding'] = 'gzip, deflate'
        self.session.headers['User-Agent'] = 'ozonapp_android'
        self.session.headers['Accept'] = 'application/json; charset=utf-8'
        # self.session.headers['Host'] = 'api.ozon.ru'
        # self.session.headers['Connection'] = 'close'
        # self.session.headers['Authorization'] = 'Bearer 3.0.Ni1g1IbLSm6_zNxVuNtGXw.28.l8cMBQAAAABjv7CpM-4Iu6phbmRyb2lkYXBwoDmAkKA..20230206133757.fLLcdYrha7VWIParasKREV8Wx4baAr8Ofp8Ry1D_qL4'
        self.session.headers['X-O3-Sample-Trace'] = 'false'
        self.session.headers['X-O3-App-Name'] = 'ozonapp_android'
        # self.session.headers['X-O3-App-Version'] = '15.0(2272)'
        # self.session.headers['X-O3-Fp'] = 'Y6Kmew3iVaAPD4Tn1ECWaSK8sh31YBKyLDbl3Bz3K45hExx+BKRW'
        # self.session.headers['Mobile-Gaid'] = '8545bbb8-93f2-417d-911e-d7461b394ee8'
        # self.session.headers['Mobile-Lat'] = '0'


        # self.session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv: 109.0) Gecko/20100101 Firefox/109.0'
        # self.session.headers['Accept'] = \
        #         'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        # # del self.session.headers['Accept']
        # self.session.headers['Alt-Used'] = 'api.ozon.ru'
        # self.session.headers['Sec-Fetch-Dest'] = 'document'
        # self.session.headers['Sec-Fetch-Mode'] = 'navigate'
        # self.session.headers['Sec-Fetch-Site'] = 'none'
        # self.session.headers['Sec-Fetch-User'] = '?1'
        # self.session.headers['Upgrade-Insecure-Requests'] = '1'
        # self.session.headers['Host'] = 'api.ozon.ru'
        # self.session.headers['Connection'] = 'close'
        # self.session.headers['Sec-Ch-Ua'] = '"Chromium";v="109", "Not_A Brand";v="99"'
        # self.session.headers['Sec-Ch-Ua-Platform'] = '"Windows"'
        # self.session.headers['Sec-Ch-Ua-Mobile'] = '?0'
        # icon_head = {
        #     'Accept': 'image/avif,image/webp,*/*',
        #     'Accept-Encoding': 'gzip, deflate, br',
        #     'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv: 109.0) Gecko/20100101 Firefox/109.0',
        #     # 'Host': 'api.ozon.ru',
        #     # 'Referer': 'https://api.ozon.ru/'
        # }
        # self.session.headers = icon_head
        # resp = self.session.get('https://api.ozon.ru/favicon.ico', headers=icon_head, allow_redirects=False)
        # if resp.status_code == 301:
        # try:
        #     resp = self.session.get('https://api.ozon.ru/favicon.ico', headers=icon_head, allow_redirects=True)
        # except requests.exceptions.TooManyRedirects:
        #     pass

    def start_queue(self, work: queue.Queue, search_in_description: bool):
        errors = 0
        done = 0
        result = None
        not_found = pd.DataFrame(columns=['offer',
                                          'result']).set_index('offer')
        while not work.empty():
            offer, brand = work.get()
            offer = str(offer)
            print(f'Начали {brand} {offer}')
            try:
                wait = random.randint(2, 5)
                print(f'Ожидание - {wait} секунд')
                time.sleep(wait)
                search_result, data = self.search(f'{offer} {brand}'.strip())
                print(f'Данные получены - {search_result}')
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
                print(Ex)
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
                # verify=False,
                # headers=head
            )
            data = resp.json()
            key = [c for c in data["widgetStates"].keys() if ('searchResultsV2' in c) or ('searchResultsError' in c)][0]
            if 'error' in key.lower():
                return False, []
            else:
                offers = json.loads(data["widgetStates"][key])['items']
                return True, offers
        except Exception as ex:
            logging.exception(ex)
            return False, 'Ошибка запроса данных о номенклатурах'

    @staticmethod
    def find_correct_links(text, offers, search_in_description):
        find_count = 1
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
        print(f'Начали распознавание, количество карточек - {len(offers)}')
        for offer_counter, offer in enumerate(offers):
            try:
                finded = []
                offer_name = [
                    c for c in offer['mainState'] if ('id' in c.keys()) and (c['id'] == 'name')
                ] \
                    [0]['atom']['textAtom']['text'].replace('&#x2F;', '/').replace('&#34;', '"').replace('&#39;', "'")
                if find_count == 4:
                    print(f'Найдено совпадений - {find_count - 1}, разрываем цикл. всего перебрано вариантов - {offer_counter}')
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
                        print(f'Начали сбор {offer_counter + 1} карточки. Ожидание - {time_to_sleep}c.')
                        time.sleep(time_to_sleep)
                        advanced_data = requests.get(f'https://api.ozon.ru/composer-api.bx/page/json/v2?url={link}').json()
                        description = json.loads(advanced_data['seo']['script'][0]['innerHTML'])['description']
                        if description:
                            print('Описание получено')
                        if standartize(text) in standartize(description):
                            finded.append('описание')
                    if finded:
                        print('Найдено совпадение')
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
                            print(f'Finded rating {rating}')
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
                        if sku == '821456315':
                            print(sku)
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
                raise
        return correct_links.reset_index(drop=False).set_index('offer')