#!/usr/local/bin/python3.9

import boto3
from botocore.config import Config
import botocore.exceptions 

import pdb
import json
import os
import itertools
import argparse
import subprocess
from lxml import etree

import asyncio, aiohttp

from typing import List, Tuple, Any, Optional, Dict


"""
The relevant data for an article consists of four components:
    JSON metadata, article text, article video, article image


JSON metadata for each article are fetched first using the website's 
exposed REST API and saved to a single file. Then this program is used
to fetch content as desired.

Dependencies in fetching the data:

JSON metadata 
    => article ID
        => article HTML URL
    => article video URL

article HTML
    => article image (URL)
    => article text


In theory the article HTML can be discarded after data is extracted.



the JSON metadata is fetched separately using a shell script and stored in a file/files.
     TODO this will be done with Python

then this program is used to fetch the remaining components of the data, and the article
    metadata and text are stored in DyanamoDB.

Currently there is no mechanism to fetch the metadata from the DB for the purpose of downloading
the other data components (though this could be integrated into the article_metadata class).
So for now the intended flow is for all the desired data to be downloaded using the on-disk
JSON reference data

"""


"""
TODO
- structured logging, or at least a mechanism to collect errors and report at the end of the operation
"""


# see program setup at the end
global_config={}

            #try: 
            #    metadata_tbl.get_item(
            #        TableName='asahi-metadata',
            #        Key={ 'article_no': { 'S': article_id } }
            #    )
            #except botocore.exceptions.ClientError as e:
            #    if e.response['Error']['Code'] == 'ResourceNotFoundException':
            #        pass
            #    else:
            #        print(repr(e.response))


class article_metadata():
    def __init__(self, json_dir, category):
        self.json_dir=json_dir
        self.category=category
        self.data={}

    def load(self, cond=None):
        if cond is None:
            cond=lambda x: True

        try:
            with open(os.path.join(self.json_dir, self.category+'.json'), 'r') as f:
                data=json.load(f)
                pending={ v['article_no']: v for v in data }

                pending=dict(itertools.filterfalse(lambda x: not cond(x[1]), pending.items()))
                self.data.update(pending)

        except Exception as e:
            print(e)
            return

    def load_manual(self, md_entry):
        self.data[md_entry['article_no']]=md_entry

  
    def read(self):
        for x in self.data.values():
            yield x

    def find(self, article_id):
        return self.data[article_id]

    def __repr__(self):
        return str(self.data.keys())
 


class Asahi():
    def __init__(self, 
        categories : dict, 
        local_paths: dict,
        url_templates : dict,
        curl_proxy=None
    ):

        self.url_templates = url_templates
        self.categories = categories
        self.curl_proxy = curl_proxy

        for x in ['json', 'html', 'video', 'img']: 
            k=x + '_dir'
            setattr(self, k, local_paths[k] if k in local_paths else None)





#ddb=session.client('dynamodb', config=conf)
        self.session = boto3.Session(profile_name='ann')
        self.db_client=self.session.client('dynamodb')
        self.db_rsrc=self.session.resource('dynamodb')

    def create_tables(self, delete_existing):

        if delete_existing == True:
            for t in ['asahi-metadata', 'asahi-content']:
                try:
                    self.db_client.delete_table(TableName=t)
                    print(f'waiting for deletion of {t}')
                    self.db_client.get_waiter('table_not_exists').wait(TableName=t)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        pass
                    else:
                        print(repr(e.response))


        self.db_client.create_table(
            TableName='asahi-metadata',
            AttributeDefinitions=[
                {
                    'AttributeName': 'article_no',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'update_time',
                    'AttributeType': 'N',
                },
            ],
            KeySchema=[
                { 'AttributeName': 'article_no', 'KeyType': 'HASH', },
                { 'AttributeName': 'update_time', 'KeyType': 'RANGE', }
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        self.db_client.create_table(
            TableName='asahi-content',
            AttributeDefinitions=[
                {
                    'AttributeName': 'article_no',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                { 'AttributeName': 'article_no', 'KeyType': 'HASH', },
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        print('waiting for table creation')
        self.db_client.get_waiter('table_exists').wait(TableName='asahi-metadata')
        self.db_client.get_waiter('table_exists').wait(TableName='asahi-content')


    def download_metadata(self, category):
        pass


    def _dl(self, url, to_file=None):
        args=[ 'curl' ]

        if self.curl_proxy:
            args.extend([ '--proxy', self.curl_proxy ])
        
        args.append('--no-progress-meter')

        if to_file:
            args.extend(['-o', to_file])

        args.append(url)

        x=subprocess.run(args)


    def _html_path(self, category, article_id):
        return os.path.join(self.html_dir, category, article_id+'.html')

    """
    shared functionality among the downloaders for the different types of data
    (images, html, video)

    dst_dir needs to be the actual destination directory including the category name
    (if applicable)
    """
    async def _generic_downloader(self, md : article_metadata, dst_dir, url_f, sleep_time=5):
        for metadata_item in md.read():
            article_id=metadata_item['article_no']

            url=url_f(md, article_id)
            if url is None:
                print(f'no URL found for article_id={article_id} (url_f returned None)')
                continue

            try:
                os.mkdir(dst_dir)
            except FileExistsError:
                pass

            dst_path=os.path.join(dst_dir, os.path.basename(url))
            try:
                os.stat(dst_path)
                print(f'skipping: already exists: {dst_path}')
                continue
            except FileNotFoundError:
                pass

            try:
                self._dl(url, to_file=dst_path)
            except Exception as e:
                print(f'failed for {article_id}({url}): {e}')
                continue

            print(f'{article_id}: completed download {url} -> {dst_path}')

            await asyncio.sleep(sleep_time)


    """
        article image URLs are provided in the article HTML
        so images should be downloaded after the article HTML files
    """
    async def download_images(self, md : article_metadata, sleep_time=5):

        def f(md, article_id):
            html_path=self._html_path(md.category, article_id)
            data=self.parse_article_html(html_path)

            if data is None:
                print('download_images: parse_article_html returned None, skipping')
                return None

            url=data['image']
            return url

        img_dir=os.path.join(self.img_dir, md.category)
        await self._generic_downloader(md, img_dir, f, sleep_time=sleep_time)
        


    async def download_articles_html(self, md : article_metadata, sleep_time=5):
        def f(md, article_id):
                url = self.url_template['html'] % (md.category, article_id)
                return url

        html_dir=os.path.join(self.html_dir, md.category)
        await self._generic_downloader(md, html_dir, f, sleep_time=sleep_time)



    async def download_videos(self, md : article_metadata, sleep_time=15):
        def f(md, article_id):
            md_entry=md.find(article_id)
            url=md_entry['movie']
            if url == '':
                print(f'download_videos: no video associated with {article_id}, skipping')
                return None

            return url

        video_dir=os.path.join(self.video_dir, md.category)
        await self._generic_downloader(md, video_dir, f, sleep_time=sleep_time)


    #
    def parse_article_html(self, path):
        try:
            os.stat(path)
        except FileNotFoundError:
            print(f'parse_article_html: {path} does not exist')
            return None

        try:
            tree=etree.parse(path, etree.HTMLParser(encoding='utf-8'))
        except OSError:
            print(f'parse failed for {path}')
            return None

        for elem in tree.xpath('/html/head/script'):
            text=elem.text
            if text is None:
                continue

            try: 
                j=json.loads(text, strict=False)
            except json.decoder.JSONDecodeError as e:
                print(f'JSONDecodeError encountered when processing {path}')
                print(e)
                continue


            if "@type" in j and j['@type'] == 'NewsArticle':
                keys=['headline', 'datePublished', 'dateModified', 'description', 'image', 'video']
                #print([ j[k] for k in keys ])
                data={ k: j[k] for k in ['headline', 'datePublished', 'dateModified']}
                data['image']=j['image']['url']
                data['text']=j['description']

                return data

        print(f'parse_article_html: no data found for {path}')
        return None

    
    """
        Iterate over article metadata JSON previously saved by shell scripts
        (1) Read in article's HTML file and extract the article contents
        (2) Insert the metadata and article contents as JSON
    """
    def store_articles(self, md : article_metadata):
        metadata_tbl=self.db_rsrc.Table('asahi-metadata')
        content_tbl=self.db_rsrc.Table('asahi-content')

        strkeys=['update_time','category_id']

        for metadata_item in md.read():
            print('processing %s' % metadata_item['article_no'])

            for k in strkeys:
                metadata_item[k]=int(metadata_item[k])

            article_id=metadata_item['article_no']
            try:
                data=self.parse_article_html(self._html_path(md.category, article_id))
            except OSError:
                print('failed reading HTML for ID %s' % metadata_item['article_no'])
                continue

            if data is None:
                print(f'store_articles: parse_article_html returned None for {article_id}, skipping')
                continue


            metadata_tbl.put_item(
                Item=metadata_item,
            )

            data['article_no']=article_id
            content_tbl.put_item(
                Item=data
            )

 

