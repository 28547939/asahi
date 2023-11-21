#!/usr/local/bin/python3.9

import json
import os
import itertools
import argparse
import subprocess

import asyncio

from typing import List, Tuple, Any, Optional, Dict


import asahi


handler_keys=[
    'download-metadata',
    'download-images',
    'download-video',
    'download-articles',
    'store-articles'
]

handlers = { k: None for k in handler_keys }


#obj.create_tables()

#obj.store_articles(md)

async def main():

    prs=argparse.ArgumentParser(
        prog='',
        description='',
    )

    prs.add_argument('--config', required=True)
    prs.add_argument('--category', required=True)
    prs.add_argument('--sleep-time')
    
    subprs=prs.add_subparsers(required=True)
    subprs_inst={}

    for cmd in handlers.keys():
        sp = subprs.add_parser(cmd).set_defaults(cmd=cmd)
        subprs_inst[cmd]=sp

    # add any command-specific arguments here by looking the command up in subprs_inst

    args=vars(prs.parse_args())

    category=args['category']
    cmd=args['cmd']
    sleep_time=args['sleep_time']

    with open(args['config'], 'rb') as f:
        config=json.load(f)

        categories=config['categories']
        local_paths=config['local_paths']
        url_templates=config['url_templates']

    obj=asahi.Asahi(categories, local_paths, url_templates)
    


    if cmd != 'download-metadata':
        md=asahi.article_metadata(local_paths['json'], category)

    async def download_metadata(): 
        obj.download_metadata(sleep_time)

    async def download_articles(): 
        obj.download_articles_html(md, sleep_time)

    async def download_images(): 
        obj.download_images(md, sleep_time)

    async def download_video(): 
        obj.download_videos(md, sleep_time)

    async def store_articles(): 
        obj.store_articles(md)

    handlers['download-metadata'] = download_metadata
    handlers['download-articles'] = download_articles
    handlers['download-images'] = download_images
    handlers['download-video'] = download_video
    handlers['store-articles'] = store_articles

    await handlers[cmd]()


if __name__ == '__main__':

    loop=asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

