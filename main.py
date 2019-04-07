# -*- coding: utf-8 -*-

from itertools import chain
from functools import reduce, partial
import urllib.parse
import urllib.request
import json
from copy import deepcopy
import csv
import sys

HOST_NAME = 'www.chiikinogennki.soumu.go.jp'
VERSION_NAME = 'v001'
FILE_EXT_NAME = 'json'

THIRD_CATEGORY_LIST = [
    '温泉'
]

OUTPUT_FIELDNAMES = [
    'name',
    'kana',
    'category1',
    'category2',
    'category3',
    'postal_code',
    'address_name',
    'address_kana',
]


def execute(initial, functions):
    return reduce(lambda x, f: f(x), functions, initial)


def flatten(iterable_of_iterable):
    return chain.from_iterable(iterable_of_iterable)


def flatmap(func, *iterables):
    return flatten(map(func, *iterables))


def debug(obj):
    print(obj)
    return obj


def request_url(url):
    return urllib.request.urlopen(url)


def make_count_url(category_name):
    encode_category_name = urllib.parse.quote(category_name)
    count_url = f'https://{HOST_NAME}/k-cloud-api/{VERSION_NAME}/kanko/{encode_category_name}/{FILE_EXT_NAME}?count=true'
    return {
        'req': {
            'category_name': category_name,
            'count_url': count_url,
        }
    }


def make_spot_urls(params):
    req = params['req']
    encode_category_name = urllib.parse.quote(req['category_name'])
    MAX_LIMIT = 50
    for i in range(0, req['count'], MAX_LIMIT):
        ret_params = deepcopy(params)
        ret_params['req']['spot_url'] = f'https://{HOST_NAME}/k-cloud-api/{VERSION_NAME}/kanko/{encode_category_name}/{FILE_EXT_NAME}?limit={MAX_LIMIT}&skip={i}'
        yield ret_params


def request_category_count(params):
    res = request_url(params['req']['count_url'])
    res_json = json.loads(res.read())
    params['req']['count'] = res_json['count']
    return params


def request_spots(params):
    res = request_url(params['req']['spot_url'])
    res_json = json.loads(res.read())
    for record in res_json['tourspots']:
        yield {
            'req': deepcopy(params['req']),
            'res': record
        }


def get_address(place, name_type='written'):
    l = []
    address_list = [
        safeget(place, 'pref', name_type),
        safeget(place, 'city', name_type),
        safeget(place, 'street', name_type),
        safeget(place, 'building', name_type),
    ]
    for a in address_list:
        if a is None:
            continue
        l.append(a)
    return ''.join(l)


def safeget(dct, *keys):
    for k in keys:
        try:
            dct = dct[k]
        except KeyError:
            return None
    return dct


def formatted_dict(params):

    res = params['res']
    genres = safeget(res, 'genres')
    try:
        category1 = genres[0]['L']
    except:
        category1 = ''
    try:
        category2 = genres[0]['M']
    except:
        category2 = ''
    try:
        category3 = genres[0]['S']
    except:
        category3 = ''
    place = safeget(res, 'place')
    return {
        'name': safeget(res, 'name', 'name1', 'written'),
        'kana': safeget(res, 'name', 'name1', 'spoken'),
        'category1': category1,
        'category2': category2,
        'category3': category3,
        'postal_code': safeget(place, 'postal_code'),
        'address_name': get_address(place, 'written'),
        'address_kana': get_address(place, 'spoken'),
    }


def main():
    writer = csv.DictWriter(sys.stdout, OUTPUT_FIELDNAMES, delimiter='\t')
    for record in execute(THIRD_CATEGORY_LIST, [
        partial(map, make_count_url),
        partial(map, request_category_count),
        partial(flatmap, make_spot_urls),
        partial(flatmap, request_spots),
        partial(map, formatted_dict),
    ]):
        writer.writerow(record)


if __name__ == "__main__":
    main()
