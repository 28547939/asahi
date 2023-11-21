#!/bin/sh

set -o nounset

cname=$1

base=/home/ann
CURL="curl --proxy socks5h://127.0.0.1:8073 --no-progress-meter"

cid=$(cat $base/cat-def.json | jq ".$cname")

if [ $cid == "null" ]; then
    echo cid is null
    exit 1
fi

if [ -z $cid ]; then
    echo cid is empty
    exit 1
fi

mkdir -p $base/json/$cname

echo $cname '->' $cid

dl () {
    istr=$(printf '%04d' $1)
    url="https://news.tv-asahi.co.jp/api/newslist.php?appcode=n4tAMkmEnY&category_id=${cid}&page=${istr}&kind=all" 
    path=/home/ann/json/$cname/page${istr}.json
    $CURL $url | jq  . > $path
    echo $path
}

# download the first page, which will also indicate the total number of pages
page1path=$(dl 1)
max_page=$(cat $page1path | jq .max_page)
echo max_page=$max_page

for i in $(seq 2 $max_page); do 
    sleep 10
    echo downloading page $i
    dl $i
done

# concatenate the pagination into one file
jq -n '[inputs.item[]]' $base/json/$cname/page*.json > $base/json/$cname.json
