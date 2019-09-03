import json
import logging
import time
import datetime
import os
import asyncio
import aiohttp
import async_timeout
import websockets
from aioredis import Redis, create_redis
from json import JSONDecodeError
from typing import Callable

DEFAULT_REDIS_URL = 'redis://127.0.0.1:6379/0'
DEFAULT_QUOTATIONS_URL = 'https://ratesjson.fxcm.com/DataDisplayer'
DEFAULT_QUOTATIONS_GET_PERIOD = 1


class Fetcher:
    def __init__(self, url, timeout=30):
        self.url_to_collect = url
        self.time_out = timeout

    async def fetch(self, session, url=None):
        if url is None:
            url = self.url_to_collect
        try:
            with async_timeout.timeout(self.time_out):
                async with session.get(url) as response:
                    return await response.text(response.charset)
        except asyncio.TimeoutError:
            return None


class RedisQuotationsClient(Redis):
    DEFAULT_EXPIRE = int(datetime.timedelta(minutes=30).total_seconds())
    ASSETS_LIST = 'assets'
    CHANNELS_PREFIX = 'chan:'

    def __init__(self, *args, expire=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._expire_data_timeout = \
            self.DEFAULT_EXPIRE if expire is None else expire

    # @classmethod
    # def from_url_with_expire_data(cls, url, expire_data_timeout, db=None, **kwargs):
    #     connection_pool = ConnectionPool.from_url(url, db=db, **kwargs)
    #     return cls(expire_data_timeout=expire_data_timeout, connection_pool=connection_pool)

    @classmethod
    def channel_full_name(cls, channel_short_name):
        return "{}{}".format(cls.CHANNELS_PREFIX, channel_short_name)

    async def set(self, *args, ex=None, **kwargs):
        if ex is None:
            ex = self._expire_data_timeout
            # kwargs['expire'] = self._expire_data_timeout
        return await super(RedisQuotationsClient, self).set(*args, expire=ex,  **kwargs)

    async def set_asset(self, asset_name, asset_time, asset_value, asset_id):
        key_field = "{}:{}".format(asset_name, asset_time)
        asset_point = {"assetName": asset_name, "time": asset_time, "assetId": asset_id, "value": asset_value}
        asset_point_json = json.dumps(asset_point)
        return await self.set(key_field, asset_point_json)

    async def get_assets(self, start=0, end=-1):
        assets = await self.lrange(self.ASSETS_LIST, start, end)
        assets = [a.decode() for a in assets]
        return assets

    async def get_asset(self, asset_id: int):
        assets = await self.get_assets(asset_id, asset_id)
        if assets:
            return assets[0]

    async def get_all_points_by_asset_name(self, asset_name):
        points = []
        keys = await self.keys("{}*".format(asset_name))
        for key in keys:
            point = await self.get(key)
            points.append(json.loads(point.decode()))
        return points

    async def push_assets(self, asset_name, *asset_names):
        return await self.lpush(self.ASSETS_LIST, asset_name, *asset_names)

    async def publish_asset(self, asset_name, asset_time, asset_value, asset_id):
        o = {"assetName": asset_name, "time": asset_time, "assetId": asset_id, "value": asset_value}
        _json = json.dumps(o)
        await self.publish(self.channel_full_name(asset_name), _json)

    async def subscribe_to_asset(self, channel):
        return await self.subscribe(self.channel_full_name(channel))

    async def unsubscribe_from_asset(self, channel):
        return await self.unsubscribe(self.channel_full_name(channel))


class QuotationWebSocketProtocol(websockets.WebSocketServerProtocol):
    ASSETS_ACTION_NAME = 'assets'
    SUBSCRIBE_ACTION_NAME = 'subscribe'
    ACTIONS_LIST = [ASSETS_ACTION_NAME, SUBSCRIBE_ACTION_NAME]

    def __init__(self, *args, **kwargs):
        super(QuotationWebSocketProtocol, self).__init__(*args, **kwargs)
        self.redis_url = os.getenv('REDIS_URL', DEFAULT_REDIS_URL)
        self.redis_client = None
        self.redis_client_coroutine = create_redis(self.redis_url, commands_factory=RedisQuotationsClient)
        self.subscribes = {}

    @staticmethod
    def build_action_response(action_name, action_message):
        return {"action": action_name, "message": action_message}

    async def do_action(self, action_dict: dict):
        action_name = action_dict.get('action')
        message = action_dict.get('message')
        if action_name is None or action_name not in self.ACTIONS_LIST:
            return
        action = getattr(self, action_name, None)
        if action is None or not isinstance(action, Callable):
            return
        message = await action(message)
        if message is None:
            return
        action_res = self.build_action_response(action_name, message)
        return action_res

    async def assets(self, message):
        if not message:
            assets = await self.redis_client.get_assets()
            message = {"assets": [{"id": i, "name": assets[i]} for i in range(len(assets))]}
            return message

    async def subscribe(self, message):
        await self.unsubscribe_all()
        asset_id = message.get('assetId')
        if asset_id is None:
            return
        asset_name = await self.redis_client.get_asset(asset_id)
        history = {"points": await self.redis_client.get_all_points_by_asset_name(asset_name)}
        history = self.build_action_response('asset_history', history)
        history_json = json.dumps(history)
        await self.send(history_json)

        sub_redis = await create_redis(self.redis_url, commands_factory=RedisQuotationsClient)
        channels = await sub_redis.subscribe_to_asset(asset_name)
        channel = channels[0]
        tsk = asyncio.ensure_future(self.channel_listener(channel, self))
        self.subscribes[asset_name] = (tsk, sub_redis)

    async def channel_listener(self, channel, ws):
        while await channel.wait_message():
            msg = await channel.get_json()
            point_dict = self.build_action_response('point', msg)
            point_json = json.dumps(point_dict)
            await ws.send(point_json)
        else:
            logging.debug('channel_listener.wait_message() return False')

    async def unsubscribe_all(self):
        for asset, tsk_and_redis in self.subscribes.items():
            tsk, redis = tsk_and_redis
            await redis.unsubscribe_from_asset(asset)
            tsk.cancel()
        self.subscribes.clear()


async def ws_handler(websocket: QuotationWebSocketProtocol, _):
    websocket.redis_client = await websocket.redis_client_coroutine
    try:
        async for message in websocket:
            try:
                action_dict = json.loads(message)
                resp = await websocket.do_action(action_dict)
                if resp is not None:
                    await websocket.send(json.dumps(resp))
            except JSONDecodeError as json_e:
                logging.debug(json_e)
    except websockets.ConnectionClosedError as con_e:
        logging.debug(con_e)


async def quotations_collect(url, period, redis_url):
    redis_quotations = await create_redis(redis_url, commands_factory=RedisQuotationsClient)
    loop = asyncio.get_event_loop()
    fetcher = Fetcher(url, period)
    current_names_of_assets = await redis_quotations.get_assets()
    async with aiohttp.ClientSession() as session:
        while True:
            start_time = time.monotonic()
            body_str = await fetcher.fetch(session)
            if body_str is None:
                continue
            prepared_to_json = body_str[body_str.index('{'):body_str.rindex('}') + 1].replace(',}', '}')
            response_dict = json.loads(prepared_to_json)
            if 'Rates' not in response_dict:
                raise ValueError("Response not have 'Rates'")
            time_stamp = datetime.datetime.utcnow().timestamp()
            assets_names_for_push = []
            for rate in response_dict['Rates']:
                assetn_name = rate['Symbol']
                asset_time = str(int(time_stamp))
                bid = float(rate['Bid'])
                ask = float(rate['Ask'])
                asset_value = (bid + ask) / 2
                if assetn_name not in current_names_of_assets:
                    assets_names_for_push.append(assetn_name)
                    current_names_of_assets.append(assetn_name)
                asset_id = current_names_of_assets.index(assetn_name)
                loop.create_task(redis_quotations.set_asset(assetn_name, asset_time, asset_value, asset_id))
                loop.create_task(redis_quotations.publish_asset(assetn_name, asset_time, asset_value, asset_id))
            if assets_names_for_push:
                loop.create_task(redis_quotations.push_assets(*assets_names_for_push))
            full_time = time.monotonic() - start_time
            wait_time = period - full_time
            if wait_time:
                await asyncio.sleep(wait_time)


async def main():
    redis_url = os.getenv('REDIS_URL', DEFAULT_REDIS_URL)
    quotations_url = os.getenv('QUOTATIONS_URL', DEFAULT_QUOTATIONS_URL)
    quotations_period = os.getenv('QUOTATIONS_GET_PERIOD', DEFAULT_QUOTATIONS_GET_PERIOD)
    # redis_quotations = await create_redis(redis_url, commands_factory=RedisQuotationsClient)
    t1 = websockets.serve(ws_handler, '0.0.0.0', 8080, klass=QuotationWebSocketProtocol)
    t2 = quotations_collect(quotations_url, quotations_period, redis_url)
    await asyncio.gather(t1, t2)


if __name__ == '__main__':
    main_loop = asyncio.get_event_loop()
    is_debug = os.getenv('DEBUG', False)
    if is_debug:
        logging.basicConfig(level=logging.DEBUG)
    try:
        asyncio.run(main(), debug=is_debug)
    except Exception as e:
        logging.debug(e)
    except KeyboardInterrupt:
        main_loop.close()
