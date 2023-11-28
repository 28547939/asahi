#!/usr/local/bin/python3.9

import json
import os
import itertools
import argparse
import subprocess

import asyncio

from typing import List, Tuple, Any, Optional, Dict


import asahi


def test_extract_article():
    ret=asahi.Asahi.parse_article_html('./data/000278054.html')
    assert(isinstance(ret, dict))

    for k in ['headline', 'datePublished', 'dateModified', 'image', 'text']:
        assert(k in ret)
        


def test_load_metadata():
    md=asahi.article_metadata('./data/json', 'subdir', 'test_category')
    md.load()

    #print(len(md.data))
    assert(len(md.data) == 273)

    # can be updated with
    # cat $METADATA | jq '.[0]| keys []' | sed -Ee 's/"//g'
    keys="""
        article_no
        category_id
        geo_flg
        img_update_time
        img_view_flag
        movie
        movie_android
        notable_news_flg
        owned_flg
        player_id
        short_title
        thumbnail
        update_time
"""

    for x in md.data:
        # discard whitespace and the two empty lines 
        for k in keys.splitlines()[1:-1]:
            if not k.lstrip() in md.data[x]:
                print(md.data[x])
                raise AssertionError(f'failed for {k}')


test_extract_article()

test_load_metadata()

