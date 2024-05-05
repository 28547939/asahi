#!/usr/local/bin/python3.9

import json
import os, sys
import itertools
import argparse
import subprocess

import asyncio

from typing import List, Tuple, Any, Optional, Dict
import decimal


import asahi


handler_keys=[
    'download-metadata',
    'download-images',
    'download-videos',
    'download-articles',
    'store-articles',
    'delete-create-tables',
    'fetch-article'
]

handlers = { k: None for k in handler_keys }


async def main():

    prs=argparse.ArgumentParser(
        prog='',
        description='',
    )

    prs.add_argument('--config', required=True)
    prs.add_argument('--sleep-time')
    prs.add_argument('--aws-profile')
    prs.add_argument('--quiet', action=argparse.BooleanOptionalAction)

    prs.set_defaults(quiet=False, sleep_time=5)

    
    subprs=prs.add_subparsers(required=True)
    subprs_inst={}

    for cmd in handlers.keys():
        sp = subprs.add_parser(cmd)
        sp.set_defaults(cmd=cmd)

        if cmd not in ['delete-create-tables', 'fetch-article']:
            sp.add_argument('--category', required=True)

        if cmd == 'fetch-article':
            sp.add_argument('--article-id', required=True)

        if cmd not in ['download-metadata', 'delete-create-tables', 'fetch-article']:
            sp.add_argument('--metadata-subdir', required=True)

        subprs_inst[cmd]=sp

    # add any command-specific arguments here by looking the command up in subprs_inst
    #   ...

    args=vars(prs.parse_args())

    cmd=args['cmd']
    sleep_time=args['sleep_time'] 
    quiet=args['quiet']


    with open(args['config'], 'rb') as f:
        config=json.load(f)

        categories=config['categories']
        local_paths=config['local_paths']
        url_templates=config['url_templates']
        curl_proxy=config['curl_proxy'] if 'curl_proxy' in config else None
        aws_profile=config['aws_profile']

    obj=asahi.Asahi(categories, local_paths, url_templates, aws_profile, curl_proxy, quiet)

    # our processing uses existing (previously downloaded) on-disk metadata except for these commands
    if cmd not in [ 'download-metadata', 'delete-create-tables', 'fetch-article' ]:
        md=asahi.article_metadata(local_paths['json'], args['metadata_subdir'], args['category'])
        md.load()

    async def download_metadata(): 
        await obj.download_metadata(args['category'], sleep_time)

    async def download_articles(): 
        await obj.download_articles_html(md, sleep_time)

    async def download_images(): 
        await obj.download_images(md, sleep_time)

    async def download_videos(): 
        await obj.download_videos(md, sleep_time)

    async def store_articles(): 
        await obj.store_articles(md)

    async def create_tables(): 
        await obj.create_tables(True)

    async def fetch_article(): 
        class J(json.JSONEncoder):
            def default(self, x):
                if isinstance(x, decimal.Decimal):
                    return float(x)

                return super().default(x)


        article_no=args['article_id']
        ret=await obj.fetch_article(article_no)
        if ret is None:
            print(f'article not found ({article_no})')
        else:
            json.dump(ret, sys.stdout, indent=4, cls=J, ensure_ascii=False)
            

    handlers['download-metadata'] = download_metadata
    handlers['download-articles'] = download_articles
    handlers['download-images'] = download_images
    handlers['download-videos'] = download_videos
    handlers['store-articles'] = store_articles
    handlers['delete-create-tables'] = create_tables
    handlers['fetch-article'] = fetch_article

    await handlers[cmd]()


if __name__ == '__main__':

    loop=asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

