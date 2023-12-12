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
    'download-videos',
    'download-articles',
    'store-articles',
    'delete-create-tables'
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

        if cmd != 'delete-create-tables':
            sp.add_argument('--category', required=True)

        if cmd not in ['download-metadata', 'delete-create-tables']:
            sp.add_argument('--metadata-subdir', required=True)

        subprs_inst[cmd]=sp

    # add any command-specific arguments here by looking the command up in subprs_inst
    #   ...

    args=vars(prs.parse_args())

    cmd=args['cmd']
    sleep_time=args['sleep_time'] 
    quiet=args['quiet']

    if 'aws_profile' in args:
        aws_session=asahi.aws_session(args['aws_profile'])
    else:
        aws_session=None

    with open(args['config'], 'rb') as f:
        config=json.load(f)

        categories=config['categories']
        local_paths=config['local_paths']
        url_templates=config['url_templates']
        curl_proxy=config['curl_proxy'] if 'curl_proxy' in config else None

    obj=asahi.Asahi(categories, local_paths, url_templates, aws_session, curl_proxy, quiet)


    # our processing uses existing (previously downloaded) on-disk metadata except for two commands
    if cmd not in [ 'download-metadata', 'delete-create-tables' ]:
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

    handlers['download-metadata'] = download_metadata
    handlers['download-articles'] = download_articles
    handlers['download-images'] = download_images
    handlers['download-videos'] = download_videos
    handlers['store-articles'] = store_articles
    handlers['delete-create-tables'] = create_tables

    await handlers[cmd]()


if __name__ == '__main__':

    loop=asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

