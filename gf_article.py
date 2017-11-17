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
import datetime
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

class GF_Article:
    gs_login_url = 'http://www.gsdata.cn/member/login'
    gs_start_url = 'http://www.gsdata.cn/custom/comrankarc'

    headers = {
        'Host': 'www.gsdata.cn',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36',
        'Referer': 'http://www.gsdata.cn/rank/wxarc',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    }

    gf_list = {'官方自媒体榜': '62', '官方旅行榜': '196', '官方幽默榜': '197', '官方情感榜': '198', '官方教育榜': '199', '官方商业榜': '200', '官方房地产榜': '201', '官方汽车榜': '205', '官方休娱榜': '547', '官方财经榜': '4267', '官方报纸榜': '4269', '官方餐饮榜': '4271', '官方法律榜': '4277', '官方政务-团委榜': '4444', '官方政务-公安榜': '4446', '官方美容榜': '4482', '官方社会生活服务榜': '4757', '官方政务-文教榜': '5576', '官方银行榜': '9931', '官方政务-医疗卫生榜': '13553', '官方建筑榜': '13810', '官方企业榜': '26482', '官方亲子榜': '26488', '官方小学榜': '26684', '官方幼儿园榜': '26917', '官方政务-工会': '30626', '官方政务-检察榜': '41078', '官方母婴榜': '50287', '官方股票榜': '50667', '官方茶文化榜': '51861', '官方户外榜': '51862', '官方大学校园榜': '56546', '官方政务榜': '79129', '官方大学榜': '79192', '官方地域-北京榜': '369', '官方地域-上海榜': '388', '官方地域-天津榜': '394', '官方地域-重庆榜': '586', '官方地域-江苏榜': '587', '官方地域-浙江榜': '588', '官方地域-福建榜': '590', '官方地域-山东榜': '591', '官方地域-河北榜': '593', '官方地域-山西榜': '594', '官方地域-辽宁榜': '596', '官方地域-吉林榜': '613', '官方地域-河南榜': '617', '官方地域-湖北榜': '618', '官方地域-湖南榜': '621', '官方地域-四川榜': '626', '官方地域-陕西榜': '635', '官方地域-新疆榜': '653', '官方地域-江西榜': '1268', '官方地域-华北榜': '1808', '官方地域-华南榜': '4478', '官方地域-西南榜': '4479', '官方地域-西北榜': '4480', '官方地域-东北榜': '4481'}

    def __init__(self):
        self.client = MongoClient('xxx',xxx)
        self.db = self.client.crawler
        self.db.authenticate('xxx', 'xxx')
        self.redis = redis.StrictRedis(host='localhost', port=6379,db=2, password='xxx')
        self.gs_session = self.gs_login('17614263727', 'xxx')
        self.check = CheckExist('article')
        self.oss = Oss_Adapter()
        self.timeout = 5
        pass

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def gs_login(self, username, password):
        data = {'username': username, 'password': password}
        s = requests.Session()
        s.post(self.gs_login_url, data)
        return s

    def quoterepl(self, matchobj):
        pattern = re.compile('"')
        return 'onclick="' + pattern.sub('&quot;', matchobj.group(0)[9: -2]) + '">'

    def get_article(self, gid, gf_name):
        params = (
            ('gid', gid),
        )
        ret = self.gs_session.get(self.gs_start_url, headers=self.headers, params=params)
        content = re.sub('onclick=\"(.*?)\">', self.quoterepl, ret.content)
        tree = html.fromstring(content)
        article_big_pic = self.db.article_big_pic
        article_small_pic = self.db.article_small_pic
        article_el = tree.xpath('//td[@class="al"]//span/a')
        for i, el in enumerate(article_el):
            try:
                item = {}
                item['forward_num'] = 0
                item['channel'] = 'wechat'
                #item['tag']
                #item['category']
                if gid in ['369', '388', '394', '586', '587', '588', '590', '591', '593', '594', '596', '613', '617', '618', '621', '626', '635', '653', '1268']:
                    item['province'] = gf_name[13: -3]
                else:
                    item['province'] = '全国'
                    item['memo'] = gf_name
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
                info = tree.xpath('//tr[' + str(i+1) + ']/td[5]/a')[0].attrib['onclick'].split("\',\'")
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

        for i in range(2, 10):
            import time
            if int(time.strftime('%H',time.localtime())) < 15:
                deltadays = datetime.timedelta(days=2)
            else:
                deltadays = datetime.timedelta(days=1)
            today = datetime.date.today()
            date = today - deltadays
            date_param = date.strftime('%Y-%m-%d')
            params = (
                ('type', 'day'),
                ('date', date_param),
                ('gid', gid),
                ('page', str(i)),
            )
            r = self.gs_session.get('http://www.gsdata.cn/custom/ajax_comrankarc', headers=self.headers, params=params)
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
                    item['forward_num'] = 0
                    item['channel'] = 'wechat'
                    if gid in ['369', '388', '394', '586', '587', '588', '590', '591', '593', '594', '596', '613', '617', '618', '621', '626', '635', '653', '1268']:
                        item['province'] = gf_name[13: -3]
                    else:
                        item['province'] = '全国'
                        item['memo'] = gf_name
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

    def start_crawler(self):
        for key, value in self.gf_list.items():
            print key + ': ' + value
            self.get_article(value, key)

gf_article = GF_Article()
gf_article.start_crawler()
