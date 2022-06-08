from pprint import pprint
import asyncio
from aiohttp import ClientSession, ClientTimeout
from datetime import datetime
from api import get_new_items_links


host = 'https://www.olx.pl'
timeout = ClientTimeout(total=10)


async def get_user_data(offer_id):
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f'{host}/api/v1/offers/{offer_id}/') as response:
            json_resp = await response.json()
    user_data = json_resp.get('data', {}).get('user', {})
    user = {'user_id': user_data.get('id'),
            'name': user_data.get('name'),
            'user_reg': datetime.fromisoformat(user_data.get('created')),
            'is_online': user_data.get('is_online'),
            'offers': await get_user_offers_count(user_data.get('id'))}
    return user


async def get_user_offers_count(user_id) -> int:
    async with ClientSession(timeout=timeout) as session:
        async with session.get(f'{host}/api/v1/offers/?user_id={user_id}&offset=0&limit=3') as response:
            json_resp = await response.json()
    user_offers = json_resp.get('data', {})
    try:
        result = len(user_offers)
    except:
        result = 9999
    return result


async def scan_pages(offers_queue):
    offers = await get_new_items_links(2)
    waypoint_offer = offers[0]
    print(f"Стоп-объява: {waypoint_offer}")
    page = 1
    await asyncio.sleep(5)
    while True:
        page += 1
        print(f"Запрос объяв (стр.{page})")
        offers = await get_new_items_links(page)
        for i, offer in enumerate(offers):
            if offer.get('offer_id') == waypoint_offer.get('offer_id') or \
                    offer.get('offer_created') < waypoint_offer.get('offer_created'):
                print(f"Конец цикла: {offer}")
                waypoint_offer = offer
                page = 0
                # Чтобы не зачастить цикл в период низкой активности
                # if page == 2 and time.time() - start_cycle < 3:
                #     await asyncio.sleep(5)
                break
            await offers_queue.put(offer)


async def process_offers(offers_queue, results_queue):
    while True:
        offer = await offers_queue.get()
        print(f"Запрос юзера по {offer.get('offer_id')} - {offer.get('offer_created')}")
        user = await get_user_data(offer.get('offer_id'))
        if user.get('offers') <= 1:
            offer.update(user)
            pprint(offer)
            # await results_queue.put(offer)


async def main():
    results_queue = asyncio.Queue()
    offers_queue = asyncio.Queue()
    tasks = [
        asyncio.create_task(scan_pages(offers_queue)),
        asyncio.create_task(process_offers(offers_queue, results_queue)),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

asyncio.run(main())
