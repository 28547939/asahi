#!/bin/sh

set -o nounset

CURL="curl --proxy socks5h://127.0.0.1:8073 --no-progress-meter"


category=$1
content=/home/ann/content/$category

mkdir -p $content

cat json/$category.json | jq '.[] | .article_no+" "+.movie' | \
    sed -Ee 's/"//g' | \
    while read n url; do

        echo article $n video $url
        if [ -f $content/${n}.mp4 ] || [ -f $content/${n}.html ]; then
            echo exists - skipping
            continue
        fi

        $CURL -o $content/${n}.html \
            https://news.tv-asahi.co.jp/$category/articles/${n}.html 

        if [ -z $url ]; then
            echo no video 
        else 
            sleep 10
            $CURL -o $content/${n}.mp4 "$url"
            sleep 10
        fi

        sleep 10
done
