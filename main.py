#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncpg
import json

from aiohttp import ClientSession, TCPConnector
from asyncio import Queue, ensure_future, get_event_loop, sleep as asyncio_sleep
from concurrent.futures import ProcessPoolExecutor
from lxml import html
from urllib.parse import urlparse
from multiprocessing import cpu_count
from time import time as time_now

import sql


MAX_CORO = 50
MAX_QUEUE = 1000

DB_CONFIG = {
    'user': 'postgres',
    'password': '',
    'database': '',
    'host': '127.0.0.1',
}

SOCIALS = [
    'twitter.com',
    # 'facebook.com',
    # 'youtube.com',
    # 'vk.com',
    # 'linkedin.com',
    # 'github.com',
    # 'deviantart.com',
    # 'soundcloud.com',
    # 'forum.unity3d.com',
    # 'behance.net',
    # 'freelanced.com',
]

BAD_LINKS = [
    # 'vk.com/share.php',
    # 'facebook.com/sharer.php',
    'twitter.com/share',
    'twitter.com/intent/tweet',
    '/status/',
    '/statuses/',
    'twitter.com/search?q',
    'twitter.com/home?status',
    # 'youtube.com/watch?',
    # 'youtube.com/playlist?',
    # 'youtube.com/embed/',
]


class SocialsGatherer(object):
    """

    """
    def __init__(self, max_queue, max_coro):
        super().__init__()

        self.QUEUE_MAX_SIZE = max_queue
        self.CORO_MAX = max_coro

        self.loop = get_event_loop()
        self.executor = ProcessPoolExecutor(cpu_count())

        self.queue = Queue(loop=self.loop)
        self.queue.put_nowait(None)

        self.session = None

        self.connection = None
        self.save_data = []

        self.reserved_tasks = set()

        self.start_time = time_now()
        self.coro = 0
        self.count = 0

    def __del__(self):
        if self.session:
            self.session.close()
            self.session = None
        self.loop.close()

    def start(self):
        ensure_future(self.producer())
        ensure_future(self.consumer())
        ensure_future(self.terminator())
        self.loop.run_forever()

    async def producer(self):
        print(' >> producer start')

        while True:
            if self.queue.qsize() < self.QUEUE_MAX_SIZE:
                tasks = await self.get_tasks()
                if not tasks:
                    await self.queue.put(None)
                    break
                for task in tasks:
                    await self.queue.put(task)
            else:
                await asyncio_sleep(2)

        print(' >> producer END!')

    async def get_tasks(self):
        if len(self.reserved_tasks) < self.QUEUE_MAX_SIZE*5:
            if not self.connection:
                self.connection = await asyncpg.connect(**DB_CONFIG)
            values = await self.connection.fetch('{} limit {};'.format(sql.SELECT_WEBSITE, self.QUEUE_MAX_SIZE*10))

            self.reserved_tasks.update([x['website'] for x in values])

        tasks = list(self.reserved_tasks)[0:self.QUEUE_MAX_SIZE]
        self.reserved_tasks.difference_update(set(tasks))

        return tasks

    async def consumer(self):
        print(' >> consumer start')

        # check queue
        if await self.queue.get() is not None:
            raise Exception('Incorrect Queue marker! Should be a None object!')

        while True:
            if self.coro < self.CORO_MAX:
                task = await self.queue.get()
                if task is None:
                    self.queue.task_done()
                    break
                ensure_future(self.do_task(task), loop=self.loop)
                self.coro += 1
            else:
                await asyncio_sleep(1)

        print(' >> consumer END!')
        self.queue.task_done()

    async def do_task(self, url):
        result = await self.get_page(self.restore_url(url))

        data = {
            'url': url,
            'domain': urlparse(url).netloc,
        }

        if result['status'] != 200:
            data['status'] = 'error'
            if result['status']:
                data['error'] = 'get status : {}, with reason : {}'.format(result['status'], result['reason'])
            else:
                data['error'] = result['error']
        else:
            result = await self.loop.run_in_executor(self.executor, self.get_socials, result['data'])
            if not result['status']:
                data['status'] = 'error'
                data['error'] = result['error']
            else:
                data['status'] = 'OK'
                data['links'] = result['data']

        await self.save_to_db(data)

        self.queue.task_done()
        self.coro -= 1

        self.count += 1
        if not self.count % 100:
            print('ready {} in {} sec'.format(self.count, (time_now()-self.start_time)))

    async def get_page(self, url):
        if not self.session:
            self.session = ClientSession(connector=TCPConnector(verify_ssl=False))

        result = dict()

        try:
            async with self.session.get(url) as response:
                result['status'] = response.status
                if response.status == 200:
                    result['data'] = await response.read()
                else:
                    result['reason'] = response.reason
        except Exception as e:
            result['status'] = 0
            result['error'] = str(e)

        return result

    @staticmethod
    def get_socials(page_html):
        try:
            page = html.fromstring(page_html)
        except Exception as e:
            return {'status': 0, 'error': str(e)}

        links = set([x.get('href') for x in page.xpath('//a')])
        result_links = []

        for link in links:
            if link and SocialsGatherer.link_is_social(link) and not SocialsGatherer.link_is_bad(link):
                result_links.append(SocialsGatherer.restore_url(link))

        return {'status': 'OK', 'data': result_links}

    async def save_to_db(self, data):
        sql_data = [
            data['url'],
            data['domain'],
        ]
        if data['status'] == 'OK':
            sql_data.append(json.dumps(data['links']))
            sql_data.append(None)
        else:
            sql_data.append(None)
            sql_data.append(data['error'])

        self.save_data.append(sql_data)

        if len(self.save_data) == 100:
            save_data = self.save_data
            self.save_data = []
            await self.connection.executemany(sql.INSERT_DATA, save_data)

    @staticmethod
    def link_is_bad(link):
        for check in BAD_LINKS:
            if check in link:
                return True
        return False

    @staticmethod
    def link_is_social(link):
        for check in SOCIALS:
            if check in link:
                return True
        return False

    @staticmethod
    def restore_url(url):
        if url[0:13] == '//twitter.com':
            url = 'http://{}'.format(url)
        if 'http://' not in url and 'https://' not in url:
            url = 'http://{}'.format(url)
        return url

    async def terminator(self):
        print(' >> terminator start')
        await self.queue.join()
        if len(self.save_data):
            self.connection.executemany(sql.INSERT_DATA, self.save_data)
        await self.connection.close()
        if self.session:
            self.session.close()
            self.session = None
        self.loop.stop()
        print(' >> TERMINATE!')


if __name__ == '__main__':
    gatherer = SocialsGatherer(MAX_QUEUE, MAX_CORO)
    gatherer.start()
