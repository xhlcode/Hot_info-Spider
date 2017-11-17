# -*- coding: utf-8 -*-
import requests
import datetime
from pymongo import MongoClient
from checkExist import CheckExist
from PIL import Image
from cStringIO import StringIO
from oss import Oss_Adapter
from bs4 import BeautifulSoup
from lxml import html
from json import *
import hashlib
import redis
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

count = 0
def add_count(self):
    global count
    count += 1

class wechat:
    gs_login_url = 'http://www.gsdata.cn/member/login'
    gs_start_url = 'http://www.gsdata.cn/rank/wxarc'
    city_info_url = 'http://gsdata-img1.oss-cn-hangzhou.aliyuncs.com/gsdata/gsdata_doc/common/js/city/citydata.json'

    headers = {
        'Host': 'www.gsdata.cn',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36',
        'Referer': 'http://www.gsdata.cn/rank/wxarc',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }

    def __init__(self):
        self.client = MongoClient('xxx',xxx)
        self.db = self.client.crawler
        self.db.authenticate('xxx', 'xxx')
        self.redis = redis.StrictRedis(host='localhost', port=6379,db=2, password='xxx')
        self.gs_session = self.gs_login('17614263727', 'xxx')
        self.check = CheckExist('article')
        self.oss = Oss_Adapter()
        self.city_info = self.get_city_info()
        self.timeout = 5
        pass

    def gs_login(self, username, password):
        data = {'username': username, 'password': password}
        s = requests.Session()
        s.post(self.gs_login_url, data)
        return s

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def quoterepl(self, matchobj):
        pattern = re.compile('"')
        return 'onclick="' + pattern.sub('&quot;', matchobj.group(0)[9: -2]) + '">'

    def get_hot_article(self):
        ret = self.gs_session.get(self.gs_start_url)
        content = re.sub('onclick=\"(.*?)\">', self.quoterepl, ret.content)
        tree = html.fromstring(content)
        article_big_pic = self.db.article_big_pic
        article_small_pic = self.db.article_small_pic
        article_el = tree.xpath('//td[@class="al"]//span/a')
        for i, el in enumerate(article_el):
            try:
                item = {}
                item['channel'] = 'wechat'
                item['province'] = '全国'
                item['title'] = el.xpath('string(.)').encode('utf-8').strip()
                item['url'] = el.attrib['href']
                info = tree.xpath('//tr[' + str(i+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
                item['source'] = info[-3]
                item['source_detail'] = info[-2].replace('&quot;', '"')
                item['original_time'] = info[-1][: -3]
                if not self.check.checkExist(item['title'] + item['source']):
                    print 'current article exist'
                    continue
                detail = requests.get(item['url'], timeout=self.timeout).text
                cover_pattern = re.compile(r'msg_cdn_url = \"([\s\S]*?)\"')
                item['cover'] = []
                cover = self.oss.uploadImageUrl(cover_pattern.findall(detail)[0])
                if not cover:
                    print 'cover upload error'
                    continue
                item['cover'].append(cover)
                item['cover'] = JSONEncoder().encode(item['cover'])
                try:
                    response = requests.get(cover_pattern.findall(detail)[0], timeout=2)
                except:
                    print 'time out'
                    continue
                img = Image.open(StringIO(response.content))
                if img.size[0]/100*100 == img.size[1]/100*100:
                    istop = 0
                else:
                    istop = 1
                abstract_pattern = re.compile(r'msg_desc = \"([\s\S]*?)\"')
                item['abstract'] = abstract_pattern.findall(detail)[0]
                content_pattern = re.compile(r'<div id="page-content" class="rich_media_area_primary">([\s\S]*?)<script nonce')
                soup = BeautifulSoup('<div id="page-content" class="rich_media_area_primary">' + content_pattern.findall(detail)[0],'lxml')
                for img in soup.find_all('img'):
                    dataSrc = img.get('data-src')
                    tp = str(img.get('data-type'))
                    ossValue = self.oss.uploadImageUrl(dataSrc, item['url'], tp=tp)
                    img['src'] = ossValue
                    del img['data-src']
                isVideo = False
                for iframe in soup.find_all('iframe'):
                    print 'video: ' + item['url']
                    isVideo = True
                    dataSrc = iframe.get('data-src')
                    iframe['src'] = dataSrc
                    del iframe['data-src']
                if isVideo:
                    continue
                import HTMLParser
                t = HTMLParser.HTMLParser()
                for p in soup.find_all('p'):
                    try:
                        p.string.replace(' ', '&nbsp;')
                        p.string.replace('　　', '&#12288;&#32;')
                        p.string = t.unescape(p.string)
                    except:
                        continue
                soup = soup.encode('utf-8', 'strict')
                item['content'] = self.oss.getName('page') + '.html'
                fp = file('tpl.html')
                lines = []
                for line in fp:
                    lines.append(line)
                fp.close()
                lines.insert(52, soup[soup.find('<div'): soup.find('</body>')])
                s = '\n'.join(lines)
                s = re.sub(r'<title>([\s\S]*?)</title>', '<title>' + item['title'] + '</title>',s)
                if not self.oss.uploadPage(s, item['content']):
                    print 'page upload error'
                    continue
                read_num = tree.xpath('//tr[' + str(i+1) + ']/td[3]/text()')[0]
                item['read_num'] = 100001 if read_num == '10W+' else int(read_num)
                up_num = tree.xpath('//tr[' + str(i+1) + ']/td[4]/text()')[0]
                item['up_num'] = 100001 if up_num == '10W+' else int(up_num)
                item['forward_num'] = int(item['up_num']/8)
                item['original_time'] = info[-1][1: -3]
                item['status'] = 1
                import time
                item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                item['update_at'] = item['created_at']
                add_count(self)
                print str(count) + '.' + item['source_detail'] + ': ' + item['url']
                if istop == 1:
                    item['id'] = self.redis.incr('article_big_pic_id')
                    article_big_pic.insert_one(item)
                else:
                    item['id'] = self.redis.incr('article_small_pic_id')
                    article_small_pic.insert_one(item)
                md5 = self.md5Encode(item['title'] + item['source'])
                self.redis.zadd('article_unique_timer', int(time.time()), md5)
                print 'insert success'
            except Exception, e:
                print str(e)
                continue
        for i in range(2, 10):
            url = 'http://www.gsdata.cn/rank/ajax_wxarc?post_time=2&page=' + str(i) + '&types=all&industry=all&proName='
            params = (
                ('post_time', '2'),
                ('page', str(i)),
                ('types', 'all'),
                ('industry', 'all'),
                ('proName', ''),
            )
            r = self.gs_session.get('http://www.gsdata.cn/rank/ajax_wxarc',headers=self.headers, params=params)
            data = json.loads(r.content)['data']
            data = re.sub('onclick=\"(.*?)\">', self.quoterepl, data)
            tree = html.fromstring(data)
            article_el = tree.xpath('//td[@class="al"]//span/a')
            for j, el in enumerate(article_el):
                try:
                    item = {}
                    item['channel'] = 'wechat'
                    item['province'] = '全国'
                    item['title'] = el.xpath('string(.)').encode('utf-8').strip()
                    item['url'] = el.attrib['href']
                    info = tree.xpath('//tr[' + str(i+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
                    item['source'] = info[-3]
                    item['source_detail'] = info[-2].replace('&quot;', '"')
                    item['original_time'] = info[-1][: -3]
                    if not self.check.checkExist(item['title'] + item['source']):
                        print 'current article exist'
                        continue
                    detail = requests.get(item['url'], timeout=self.timeout).text
                    cover_pattern = re.compile(r'msg_cdn_url = \"([\s\S]*?)\"')
                    item['cover'] = []
                    cover = self.oss.uploadImageUrl(cover_pattern.findall(detail)[0])
                    if not cover:
                        print 'cover upload error'
                        continue
                    item['cover'].append(cover)
                    item['cover'] = JSONEncoder().encode(item['cover'])
                    try:
                        response = requests.get(cover_pattern.findall(detail)[0], timeout=2)
                    except:
                        print 'time out'
                        continue
                    img = Image.open(StringIO(response.content))
                    if img.size[0]/100*100 == img.size[1]/100*100:
                        istop = 0
                    else:
                        istop = 1
                    abstract_pattern = re.compile(r'msg_desc = \"([\s\S]*?)\"')
                    item['abstract'] = abstract_pattern.findall(detail)[0]
                    content_pattern = re.compile(r'<div id="page-content" class="rich_media_area_primary">([\s\S]*?)<script nonce')
                    soup = BeautifulSoup('<div id="page-content" class="rich_media_area_primary">' + content_pattern.findall(detail)[0],'lxml')
                    for img in soup.find_all('img'):
                        dataSrc = img.get('data-src')
                        tp = str(img.get('data-type'))
                        ossValue = self.oss.uploadImageUrl(dataSrc, item['url'], tp=tp)
                        img['src'] = ossValue
                        del img['data-src']
                    isVideo = False
                    for iframe in soup.find_all('iframe'):
                        print 'video: ' + item['url']
                        isVideo = True
                        dataSrc = iframe.get('data-src')
                        iframe['src'] = dataSrc
                        del iframe['data-src']
                    if isVideo:
                        continue
                    import HTMLParser
                    t = HTMLParser.HTMLParser()
                    for p in soup.find_all('p'):
                        try:
                            p.string.replace(' ', '&nbsp;')
                            p.string.replace('　　', '&#12288;&#32;')
                            p.string = t.unescape(p.string)
                        except:
                            continue
                    soup = soup.encode('utf-8', 'strict')
                    item['content'] = self.oss.getName('page') + '.html'
                    fp = file('tpl.html')
                    lines = []
                    for line in fp:
                        lines.append(line)
                    fp.close()
                    lines.insert(52, soup[soup.find('<div'): soup.find('</body>')])
                    s = '\n'.join(lines)
                    s = re.sub(r'<title>([\s\S]*?)</title>', '<title>' + item['title'] + '</title>',s)
                    if not self.oss.uploadPage(s, item['content']):
                        print 'page upload error'
                        continue
                    read_num = tree.xpath('//tr[' + str(j+1) + ']/td[3]/text()')[0]
                    item['read_num'] = 100001 if read_num == '10W+' else int(read_num)
                    up_num = tree.xpath('//tr[' + str(j+1) + ']/td[4]/text()')[0]
                    item['up_num'] = 100001 if up_num == '10W+' else int(up_num)
                    item['forward_num'] = int(item['up_num']/8)
                    info = tree.xpath('//tr[' + str(j+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
                    item['source'] = info[-3]
                    item['source_detail'] = info[-2].replace('&quot;', '"')
                    item['original_time'] = info[-1][: -3]
                    item['status'] = 1
                    import time
                    item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    item['update_at'] = item['created_at']
                    add_count(self)
                    print str(count) + '.' + item['source_detail'] + ': ' + item['url']
                    if istop == 1:
                        item['id'] = self.redis.incr('article_big_pic_id')
                        article_big_pic.insert_one(item)
                    else:
                        item['id'] = self.redis.incr('article_small_pic_id')
                        article_small_pic.insert_one(item)
                    md5 = self.md5Encode(item['title'] + item['source'])
                    self.redis.zadd('article_unique_timer', int(time.time()), md5)
                    print 'insert success'
                except:
                    continue

    def get_city_info(self):
        ret = self.gs_session.get(self.city_info_url)
        result = json.loads(ret.content)['citys']
        city_info = []
        for data in result:
            city_info.append(data['provinceName'] + ' ' + data['name'])
        return city_info

    def get_city_article(self):
        article_big_pic = self.db.article_big_pic
        article_small_pic = self.db.article_small_pic
        for city in self.city_info:
            city_info = city.split()
            print city_info[0] + ': ' + city_info[1]
            for i in range(1, 10):
                params = (
                    ('post_time', '2'),
                    ('page', str(i)),
                    ('types', 'all'),
                    ('industry', city_info[1]),
                    ('proName', city_info[0]),
                )
                r = self.gs_session.get('http://www.gsdata.cn/rank/ajax_wxarc',headers=self.headers, params=params)
                try:
                    data = json.loads(r.content)['data']
                except:
                    print 'no more article'
                    break
                data = re.sub('onclick=\"(.*?)\">', self.quoterepl, data)
                tree = html.fromstring(data)
                article_el = tree.xpath('//td[@class="al"]//span/a')
                for j, el in enumerate(article_el):
                    try:
                        item = {}
                        item['channel'] = 'wechat'
                        item['city'] = city_info[1]
                        item['province'] = city_info[0]
                        item['title'] = el.xpath('string(.)').encode('utf-8').strip()
                        item['url'] = el.attrib['href']
                        info = tree.xpath('//tr[' + str(j+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
                        item['source'] = info[-3]
                        item['source_detail'] = info[-2].replace('&quot;', '"')
                        item['original_time'] = info[-1][: -3]
                        if not self.check.checkExist(item['title'] + item['source']):
                            print 'current article exist'
                            continue
                        detail = requests.get(item['url'], timeout=self.timeout).text
                        cover_pattern = re.compile(r'msg_cdn_url = \"([\s\S]*?)\"')
                        item['cover'] = []
                        cover = self.oss.uploadImageUrl(cover_pattern.findall(detail)[0])
                        if not cover:
                            print 'cover upload error'
                            continue
                        item['cover'].append(cover)
                        item['cover'] = JSONEncoder().encode(item['cover'])
                        try:
                            response = requests.get(cover_pattern.findall(detail)[0], timeout=2)
                        except:
                            print 'time out'
                            continue
                        img = Image.open(StringIO(response.content))
                        if img.size[0]/100*100 == img.size[1]/100*100:
                            istop = 0
                        else:
                            istop = 1
                        abstract_pattern = re.compile(r'msg_desc = \"([\s\S]*?)\"')
                        item['abstract'] = abstract_pattern.findall(detail)[0]
                        content_pattern = re.compile(r'<div id="page-content" class="rich_media_area_primary">([\s\S]*?)<script nonce')
                        soup = BeautifulSoup('<div id="page-content" class="rich_media_area_primary">' + content_pattern.findall(detail)[0],'lxml')
                        for img in soup.find_all('img'):
                            dataSrc = img.get('data-src')
                            tp = str(img.get('data-type'))
                            ossValue = self.oss.uploadImageUrl(dataSrc, item['url'], tp=tp)
                            img['src'] = ossValue
                            del img['data-src']
                        isVideo = False
                        for iframe in soup.find_all('iframe'):
                            print 'video: ' + item['url']
                            isVideo = True
                            dataSrc = iframe.get('data-src')
                            iframe['src'] = dataSrc
                            del iframe['data-src']
                        if isVideo:
                            continue
                        import HTMLParser
                        t = HTMLParser.HTMLParser()
                        for p in soup.find_all('p'):
                            try:
                                p.string.replace(' ', '&nbsp;')
                                p.string.replace('　　', '&#12288;&#32;')
                                p.string = t.unescape(p.string)
                            except:
                                continue
                        soup = soup.encode('utf-8', 'strict')
                        item['content'] = self.oss.getName('page') + '.html'
                        fp = file('tpl.html')
                        lines = []
                        for line in fp:
                            lines.append(line)
                        fp.close()
                        lines.insert(52, soup[soup.find('<div'): soup.find('</body>')])
                        s = '\n'.join(lines)
                        s = re.sub(r'<title>([\s\S]*?)</title>', '<title>' + item['title'] + '</title>',s)
                        if not self.oss.uploadPage(s, item['content']):
                            print 'page upload error'
                            continue
                        read_num = tree.xpath('//tr[' + str(j+1) + ']/td[3]/text()')[0]
                        item['read_num'] = 100001 if read_num == '10W+' else int(read_num)
                        up_num = tree.xpath('//tr[' + str(j+1) + ']/td[4]/text()')[0]
                        item['up_num'] = 100001 if up_num == '10W+' else int(up_num)
                        item['forward_num'] = int(item['up_num']/8)
                        info = tree.xpath('//tr[' + str(j+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
                        item['source'] = info[-3]
                        item['source_detail'] = info[-2].replace('&quot;', '"')
                        item['original_time'] = info[-1][: -3]
                        item['status'] = 1
                        import time
                        item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        item['update_at'] = item['created_at']
                        add_count(self)
                        print str(count) + '.' + item['source_detail'] + ': ' + item['url']
                        if istop == 1:
                            item['id'] = self.redis.incr('article_big_pic_id')
                            article_big_pic.insert_one(item)
                        else:
                            item['id'] = self.redis.incr('article_small_pic_id')
                            article_small_pic.insert_one(item)
                        md5 = self.md5Encode(item['title'] + item['source'])
                        self.redis.zadd('article_unique_timer', int(time.time()), md5)
                        print 'insert success'
                    except Exception, e:
                        print str(e)
                        continue


wechat = wechat()
wechat.get_city_article() # 根据地区获取热门文章
wechat.get_hot_article() # 获取热门文章总排名
