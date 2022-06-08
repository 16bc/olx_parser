from pprint import pprint
import asyncio
from aiohttp import ClientSession, ClientTimeout
from datetime import datetime, timedelta
from dateutil.parser import parse


timeout = ClientTimeout(total=10)
offer_created_old = 24  # Время создания объявления (часов назад)
user_reg_old = 48  # Время регистрации пользователя (часов назад)

async def get_new_items_links(page):
    offset = page * 40
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f'https://www.olx.pl/api/v1/offers?offset={offset}&search[order]=created_at%3Adesc') as response:
            res_json = await response.json()
    promolist = res_json['metadata']['promoted']
    all_offers = res_json['data']
    result = []
    offer_thres_dt = (datetime.now() - timedelta(hours=offer_created_old)).timestamp()
    user_thres_dt = (datetime.now() - timedelta(hours=user_reg_old)).timestamp()
    for i, off in enumerate(all_offers):
        if i not in promolist \
        and parse(off['created_time']).timestamp() > offer_thres_dt \
        and parse(off['user']['created']).timestamp() > user_thres_dt:
            item = {
                'offer_id': off.get('id'),
                'offer_created_time': off.get('created_time'),
                'offer_last_refresh_time': off.get('last_refresh_time'),
                'offer_url': off.get('url'),
                'user_reg': off.get('user', {}).get('created'),
                'user_id': off.get('user', {}).get('id')
            }
            result.append(item)
    # print(all_offers[-1].get('created_time'), ' - ', all_offers[-1].get('last_refresh_time'))
    return result


async def user_offers_count_filter(input_queue, checked_offers_queue):
    """
    Берёт из первой очереди объяву и проверяет другие объявы пользователя на свежесть.
    Если объяв меньше или равно 1, или все объявы свежие, то помещает объяву в очередь <checked_offers_queue>
    :param input_queue: Входная очередь
    :param checked_offers_queue: Выходная очередь
    :return:
    """
    offer = await input_queue.get()
    user_id = offer.get('user_id')
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f'https://www.olx.pl/api/v1/offers/?user_id={user_id}&limit=3') as response:
            res_json = await response.json()
    if len(res_json.get('data')) <= 1:
        await checked_offers_queue.put(offer)
    else:  # Проверяем что другие объявы тоже свежие
        fail = False
        offer_thres_dt = (datetime.now() - timedelta(hours=offer_created_old)).timestamp()
        for off in res_json.get('data'):
            if parse(off.get('created_time')).timestamp() < offer_thres_dt:
                fail = True
                break
        if not fail:
            await checked_offers_queue.put(offer)


async def search_new_offers():
    for i in range(25):
        res_list = await get_new_items_links(i)
        pprint(len(res_list))
        for res in res_list:
            print(res.get('offer_id'), ' -- ', res.get('offer_created_time'))
        await asyncio.sleep(1)




asyncio.get_event_loop().run_until_complete(search_new_offers())


