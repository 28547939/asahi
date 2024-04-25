#!/usr/local/bin/python3.9

import boto3
from botocore.config import Config
import botocore.exceptions 

import json
import os
import itertools
import argparse
import subprocess
from lxml import etree

import asyncio, aiohttp

from typing import List, Tuple, Any, Optional, Dict

from datetime import datetime

"""
The relevant data for an article consists of four components:
    JSON metadata, article text, article video, article image


JSON metadata for each article are fetched first using the website's 
exposed HTTP API and saved to a single (concatenated) file using the download_metadata
function/command. Then other commands are used to download other data as desired.

Dependencies in fetching the data:

JSON metadata 
    => article ID
        => article HTML URL
    => article video URL

article HTML
    => article image (URL)
    => article text (contained in JSON+LD data in a <script> tag)


In theory the article HTML can be discarded after data is extracted.

Currently there is no mechanism to fetch the metadata from the DB for the purpose of downloading
the other data components (though this could be integrated into the article_metadata class).
So for now the intended flow is for all the desired data to be downloaded using the on-disk
JSON reference data (that reference data having already been downloaded using download_metadata)

"""


"""
TODO
- structured logging, or at least a mechanism to collect errors and report at the end of the operation
- function decorator to indicate required dependencies 
"""


# article metadata is just JSON that has been downloaded from the website's REST endpoint.
# it's downloaded in pages, which each page response being a JSON object containing an array
# articles (JSON objects containing article metadata) in the page.
# download_metadata removes the rest of the object, leaving the list of article objects,
# and concatenates the pages.
# 
# `load` loads a processed document (JSON list of article metadata objects) from disk
#   for use with all the download functions except download_metadata
# `load_raw_page`, below, loads an un-processed page document (also from disk)
#   for use with download_metadata
class article_metadata():
    def __init__(self, json_dir, subdir, category):
        self.json_dir=os.path.join(json_dir, subdir)
        self.category=category
        self.data={}

    def load(self, cond=None):
        if cond is None:
            cond=lambda x: True

        try:
            with open(os.path.join(self.json_dir, self.category, self.category+'.json'), 'r') as f:
                data=json.load(f)
                pending={ v['article_no']: v for v in data }

                pending=dict(filter(lambda x: cond(x[1]), pending.items()))
                self.data.update(pending)

        except Exception as e:
            raise

    # try to load one page of metadata
    @staticmethod
    def load_raw_page(path, item_key='item'):
        with open(path, 'r') as f:
            data=json.load(f)
            if data['article_count'] == 0 or not item_key in data:
                self.log(f'article_metadata.load_metadata_page: no articles found, returning None ({path})')
                return None

            return data

    def read(self):
        for x in self.data.values():
            yield x

    def find(self, article_id):
        return self.data[article_id]

    def __repr__(self):
        return str(self.data.keys())
 
class aws_session():
    def __init__(self, profile_name):
        self.session = boto3.Session(profile_name=profile_name)
        self.db_client=self.session.client('dynamodb')
        self.db_rsrc=self.session.resource('dynamodb')

        self.metadata_tbl_name='asahi-metadata'
        self.article_tbl_name='asahi-content'


class Asahi():
    def __init__(self, 
        categories : dict, 
        local_paths: dict,
        url_templates : dict,
        aws_profile=None,
        curl_proxy=None,
        quiet=False,
    ):

        self.url_templates = url_templates
        self.categories = categories
        self.curl_proxy = curl_proxy
        self.quiet = quiet
        self.aws_profile=aws_profile

        if aws_profile:
            self.aws_session = aws_session(aws_profile)

        for x in ['json', 'html', 'video', 'img']: 
            k=x + '_dir'
            setattr(self, k, local_paths[x] if x in local_paths else None)

    def log(self, msg):
        if not self.quiet:
            print(msg)



    async def create_tables(self, delete_existing):
        aws_session=self.aws_session
        mtbl=aws_session.metadata_tbl_name
        atbl=aws_session.article_tbl_name

        if delete_existing == True:
            for t in [mtbl, atbl]:
                try:
                    aws_session.db_client.delete_table(TableName=t)
                    print(f'waiting for deletion of {t}')
                    aws_session.db_client.get_waiter('table_not_exists').wait(TableName=t)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        pass
                    else:
                        print(repr(e.response))


                # TODO  dynamodb global secondary index on update_time, category
                #{
                #     'AttributeName': 'update_time',
                #    'AttributeType': 'N',
                #},

        aws_session.db_client.create_table(
            TableName=aws_session.metadata_tbl_name,
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

        aws_session.db_client.create_table(
            TableName=aws_session.article_tbl_name,
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
        aws_session.db_client.get_waiter('table_exists').wait(TableName=mtbl)
        aws_session.db_client.get_waiter('table_exists').wait(TableName=atbl)


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
    (images, html, video). 
    JSON metadata is downloaded manually using _dl

    dst_dir needs to be the actual destination directory including the category name
    (if applicable)

    url_f provides the URL, given the article_metadata object and the article ID
    """
    async def _generic_downloader(self, md : article_metadata, dst_dir, url_f, sleep_time=5):
        nonempty=None
        failed=[]

        for metadata_item in md.read():
            article_id=metadata_item['article_no']
            nonempty=True

            url=url_f(md, article_id)
            if url is None:
                print(f'no URL found for article_id={article_id} (url_f returned None)')
                continue

            try:
                os.mkdir(dst_dir)
            except FileExistsError:
                pass

            # check if the destination file already exists
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
                failed.append(article_id)
                continue

            print(f'{article_id}: completed download {url} -> {dst_path}')
            await asyncio.sleep(sleep_time)

        if len(failed) > 0:
            print('download completed. begin list of article IDs for which download failed')
            for x in failed:
                print(x)

            print('end list of article IDs for which download failed')

        if not nonempty:
            self.log('_generic_downloader: no entries present in the article_metadata object')



    async def download_metadata(self, category, sleep_time=5, item_key='item'):
        if category not in self.categories:
            raise ValueError(f'unknown category {category}')

        datestr=datetime.now().isoformat()
        subdir=category+'_'+datestr
        json_dir=os.path.join(self.json_dir, subdir, category)

        try:
            os.makedirs(json_dir)
        except FileExistsError:
            pass

        items_key='item'

        def path(page):
            return os.path.join(json_dir, ('page%04d' % page) + '.json')

        def url(page):
            page_str='%04d' % page
            return (self.url_templates['metadata'] % (self.categories[category], page_str))

        def do_dl(page):
            a=url(page)
            b=path(page)
            self.log(f'download_metadata: downloading {a} -> {b}')
            self._dl(a, to_file=b)

        def prune_existing(loaded_page):
            # optimization would be to iterate in a loop and break once an existing one is found,
            # since that signals that all articles in all further pages are existing
            # (we assume the entire article list across all pages is ordered in time)
            return list(filter(lambda item: self.fetch_article(item['article_no']) is None,
                loaded_page[item_key],
            ))

        concatenated=[]

        # check content of first page of metadata to get total number of pages
        do_dl(1)
        current_page=article_metadata.load_metadata_page(path(1))
        try:
            max_page=current_page['max_page']
        except KeyError:
            raise Exception(f'malformed first page at {path(1)}: max_page key not present')

        pruned=prune_existing(current_page)
        concatenated=pruned

        self.log(f'will download at most {max_page} total pages')


        for i in range(2, max_page + 1):
            # if we have encountered an existing article (whether in page 1 above, or
            # while iterating in the loop), we break, since we assume that we have
            # the articles in all subsequent pages
            # but check the exact difference anyway to report to the user
            if len(pruned) != len(current_page[item_key]):
                (a, b)=(
                    set([ v['article_no'] for v in x ])
                    for x in (pruned, current_page[item_key])
                )
                existing=a ^ b
                self.log(f'found existing articles in page {i}, not downloading any further pages. (existing={existing})')

                break

            do_dl(i)
            path_i=path(i)
            current_page=article_metadata.load_metadata_page(path_i)

            pruned=prune_existing(current_page)
            concatenated.extend(pruned)

            await asyncio.sleep(sleep_time)

        out_path=os.path.join(json_dir, f'{category}.json')
        if len(concatenated) == 0:
            self.log(f'no new records (not writing metadata file to {datestr})')
            return

        with open(out_path, 'w') as f:
            json.dump(concatenated, f, indent=4, ensure_ascii=False)
            self.log(f'wrote full metadata file to {out_path} (subdir is {subdir})')



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
                url = self.url_templates['html'] % (md.category, article_id)
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


    @staticmethod
    def parse_article_html(path):
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
        Iterate over article metadata JSON previously saved by the download-metadata
        command, additionally reading the article contents (HTML) saved by the 
        download-articles command.
        (1) Read in article's HTML file and extract the article contents
        (2) Insert the metadata and article contents as JSON
    """
    async def store_articles(self, md : article_metadata):
        metadata_tbl=self.aws_session.db_rsrc.Table(self.aws_session.metadata_tbl_name)
        article_tbl=self.aws_session.db_rsrc.Table(self.aws_session.article_tbl_name)

        for metadata_item in md.read():
            print('processing %s' % metadata_item['article_no'])

            # convert these entries from str to int
            for k in ['update_time','category_id']:
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
            article_tbl.put_item(
                Item=data
            )

    # TODO  incorporate sorting to get most recent article
    async def fetch_article(self, article_no):
        try:
            metadata_tbl=self.aws_session.db_rsrc.Table(self.aws_session.metadata_tbl_name)
            article_tbl=self.aws_session.db_rsrc.Table(self.aws_session.article_tbl_name)

            metadata=metadata_tbl.get_item(
                Key={
                    'article_no': article_no
                },
            )

            article=article_tbl.get_item(
                Key={
                    'article_no': article_no
                },
            )

            ret = {}

            def f (resp, k):
                if 'Item' in resp:
                    ret[k]=resp['Item']
                else:
                    print(f'warning: `Item` key not present in response ({resp})')

            f(metadata, 'metadata'),
            f(article, 'article')

            return ret

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return None
            else:
                raise e

