# -*- coding: utf-8 -*-
import requests
import datetime
from pymongo import MongoClient
from bson import ObjectId
from checkExist import CheckExist
from oss import Oss_Adapter
from PIL import Image
from cStringIO import StringIO
from lxml import html
from json import *
import hashlib
import time
import math
import redis
import json
import os
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

count = 0
def add_count(self):
    global count
    count += 1

class Video_kuaishou:
    headers = {
        'Host': 'api.ksapisrv.com',
        'X-REQUESTID': '104486756',
        'Accept': 'application/json',
        'User-Agent': 'kwai-ios',
        'Accept-Language': 'zh-Hans-CN;q=1',
    }

    params = (
        ('appver', '5.4.0.280'),
        ('did', '18A8D514-C9F7-4076-9E35-9C5574F9C7AB'),
        ('c', 'a'),
        ('ver', '5.4'),
        ('sys', 'ios11.1'),
        ('mod', 'iPhone6,1'),
        ('net', '中国联通_5'),
    )

    comment_headers = {
        'Host': '124.243.249.4',
        #'X-REQUESTID': '186891852',
        'Accept': 'application/json',
        'User-Agent': 'kwai-ios',
        'Accept-Language': 'zh-Hans-CN;q=1',
    }

    comment_params = (
        ('appver', '5.4.1.284'),
        ('did', '197E134C-17D7-4218-AAD2-E4E3D9B6499A'),
        ('c', 'a'),
        ('ver', '5.4'),
        ('sys', 'ios11.1.1'),
        ('mod', 'iPhone9,1'),
        ('net', '中国联通_5'),
    )
    comment_headers_n = {
        'Host': '124.243.249.4',
        #'X-REQUESTID': '187559148',
        'Accept': 'application/json',
        'User-Agent': 'kwai-ios',
        'Accept-Language': 'zh-Hans-CN;q=1',
    }

    comment_params_n = (
        ('appver', '5.4.1.284'),
        ('did', '197E134C-17D7-4218-AAD2-E4E3D9B6499A'),
        ('c', 'a'),
        ('ver', '5.4'),
        ('sys', 'ios11.1.1'),
        ('mod', 'iPhone9,1'),
        ('net', '中国联通_5'),
    )

    def __init__(self):
        self.client = MongoClient('xxx',xxx)
        self.db = self.client.crawler
        self.db.authenticate('xxx', 'xxx')
        self.redis = redis.StrictRedis(host='localhost', port=6379,db=2, password='xxx')
        self.check = CheckExist('video')
        self.oss = Oss_Adapter()
        self.maxCommentNum = 500
        pass

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def get_sig(self, user_id):
        s = 'appver=5.4.0.280c=aclient_key=56c3713ccount=100country_code=cndid=18A8D514-C9F7-4076-9E35-9C5574F9C7ABlanguage=zh-Hans-CN;q=1mod=iPhone6,1net=中国联通_5privacy=publicsys=ios11.1user_id=1086657ver=5.423caab00356c'
        s = re.sub(r'user_id=(\d+)', 'user_id=' + str(user_id), s)
        return self.md5Encode(s)

    def get_comment_sig_0(self, photo_id):
        s = 'appver=5.4.1.284c=aclient_key=56c3713ccountry_code=cndid=197E134C-17D7-4218-AAD2-E4E3D9B6499Alanguage=zh-Hans-CN;q=1mod=iPhone9,1net=中国联通_5photoId=3877028987sys=ios11.1.1ver=5.423caab00356c'
        s = re.sub(r'photoId=(\d+)', 'photoId=' + photo_id, s)
        return self.md5Encode(s)

    def get_comment_sig_1(self, photo_id, pcursor):
        s = 'appver=5.4.1.284c=aclient_key=56c3713ccountry_code=cndid=197E134C-17D7-4218-AAD2-E4E3D9B6499Alanguage=zh-Hans-CN;q=1mod=iPhone9,1net=中国联通_5pcursor=19495738167photoId=3858067491sys=ios11.1.1ver=5.423caab00356c'
        s = re.sub(r'photoId=(\d+)', 'photoId=' + photo_id, s)
        s = re.sub(r'pcursor=(\d+)', 'pcursor=' + pcursor, s)
        return self.md5Encode(s)

    def check_contain_chinese(self, check_str):
        for ch in check_str.decode('utf-8'):
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
        return False

    def is_hot_video(self, comment_num, watch_num, like_num, timestamp):
        return timestamp/1000 > int(time.time()) - 30 * 24 * 60 * 60
        #return comment_num * 10 + watch_num * 5 + like_num > 100000;

    def insert_video(self, item):
        video_verti = self.db.video_verti
        video_horiz = self.db.video_horiz
        result = {}
        try:
            if item['height'] > item['width']:
                item['id'] = self.redis.incr('video_verti_id')
                _id = video_verti.insert_one(item)
                result['db'] = 'verti'
            else:
                item['id'] = self.redis.incr('video_horiz_id')
                _id = video_horiz.insert_one(item)
                result['db'] = 'horiz'
            _id = str(_id.inserted_id)
            result['_id'] = _id
            md5 = self.md5Encode(item['title'] + item['source'])
            self.redis.zadd('video_unique_timer', int(time.time()), md5)
            print 'insert success'
            return result
        except:
            print 'insert error'
            return None

    def insert_comment(self, _id, comment_url, db_name):
        video_verti = self.db.video_verti
        video_horiz = self.db.video_horiz
        if db_name == 'verti':
            comment_num = video_verti.find_one({'_id': ObjectId(_id)})['comment_num']
        else:
            comment_num = video_horiz.find_one({'_id': ObjectId(_id)})['comment_num']
        print comment_num
        totalNum = self.maxCommentNum if comment_num > self.maxCommentNum else comment_num
        currentNum = 0
        sig = self.get_comment_sig_0(comment_url)
        data = [
          ('client_key', '56c3713c'),
          ('country_code', 'cn'),
          ('language', 'zh-Hans-CN;q=1'),
          ('photoId', comment_url),
          ('sig', sig),
        ]
        ret = requests.post('http://api.ksapisrv.com/rest/n/comment/list/v2', headers=self.comment_headers, params=self.comment_params, data=data)
        result = json.loads(ret.content)
        pcursor = result['pcursor']
        result = result['rootComments']
        comment = self.db.comment
        for data in result:
            try:
                item = {}
                item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                item['content'] = data['content']
                item['nickname'] = data['author_name']
                item['avatar'] = self.oss.uploadImageUrl(data['headurls'][0]['url'])
                item['up_num'] = 0
                item['item_id'] = _id
                item['original_time'] = data['time']
                item['id'] = self.redis.incr('comment_id')
                comment.insert_one(item)
                print 'insert nick_name: ' + item['nickname']
                currentNum += 1
            except Exception, e:
                print str(e)
                print 'insert error'
        while currentNum < totalNum:
            if pcursor == 'no_more':
                break
            print '--------------------'
            sig = self.get_comment_sig_1(comment_url, pcursor)
            data = [
              ('client_key', '56c3713c'),
              ('country_code', 'cn'),
              ('language', 'zh-Hans-CN;q=1'),
              ('pcursor', pcursor),
              ('photoId', comment_url),
              ('sig', sig),
            ]
            ret = requests.post('http://124.243.249.4/rest/n/comment/list/v2', headers=self.comment_headers_n, params=self.comment_params_n, data=data)
            result = json.loads(ret.content)
            pcursor = result['pcursor']
            result = result['rootComments']
            comment = self.db.comment
            for data in result:
                try:
                    item = {}
                    item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    item['content'] = data['content']
                    item['nickname'] = data['author_name']
                    item['avatar'] = self.oss.uploadImageUrl(data['headurls'][0]['url'])
                    item['up_num'] = 0
                    item['item_id'] = _id
                    item['original_time'] = data['time']
                    item['id'] = self.redis.incr('comment_id')
                    comment.insert_one(item)
                    print 'insert nick_name: ' + item['nickname']
                    currentNum += 1
                except Exception, e:
                    print str(e)
                    print 'insert error'
        if db_name == 'verti':
            video_verti.update_one({'_id': ObjectId(_id)}, {'$set': {'comment_num': currentNum}})
            doc = video_verti.find_one({'_id': ObjectId(_id)})
        else:
            video_horiz.update_one({'_id': ObjectId(_id)}, {'$set': {'comment_num': currentNum}})
            doc = video_horiz.find_one({'_id': ObjectId(_id)})
        print 'current comment_num: ' + str(doc['comment_num'])

    def get_video(self):
        author = self.db.author
        cursor = author.find({'channel': 'kuaishou', 'fan_num': {'$gte': 1000000}})
        for doc in cursor:
            sig = self.get_sig(doc['memo'])
            data = [
              ('client_key', '56c3713c'),
              ('count', '100'),
              ('country_code', 'cn'),
              ('language', 'zh-Hans-CN;q=1'),
              ('privacy', 'public'),
              ('sig', sig),
              ('user_id', doc['memo']),
            ]
            ret = requests.post('http://api.ksapisrv.com/rest/n/feed/profile2', headers=self.headers, params=self.params, data=data)
            try:
                result = json.loads(ret.content)['feeds']
            except:
                print 'no video'
                continue
            for data in result:
                try:
                    item = {}
                    item['title'] = data['caption']
                    if not self.check_contain_chinese(item['title']):
                        continue
                    item['source'] = str(data['user_id'])
                    item['source_detail'] = data['user_name']
                    item['comment_num'] = data['comment_count']
                    item['forward_num'] = int(item['comment_num']/6)
                    item['watch_num'] = data['view_count']
                    item['like_num'] = data['like_count']
                    item['width'] = data['ext_params']['w']
                    item['height'] = data['ext_params']['h']
                    if not self.is_hot_video(item['comment_num'], item['watch_num'], item['like_num'], data['timestamp']):
                        continue
                    item['channel'] = 'kuaishou'
                    try:
                        response = requests.get(data['cover_thumbnail_urls'][0]['url'], timeout = 2)
                    except:
                        print 'time out'
                        continue
                    img = Image.open(StringIO(response.content))
                    img.save('tmp_kuaishou.jpg')
                    with open('tmp_kuaishou.jpg', 'rb') as fileobj:
                        item['cover'] = self.oss.uploadImageFile(fileobj)
                    os.remove('tmp_kuaishou.jpg')
                    if item['cover'] == '':
                        print 'uploadImageFile error'
                        continue
                    item['url'] = data['main_mv_urls'][0]['url']
                    if not self.check.checkExist(item['title'] + item['source']):
                        print 'current video exist'
                        continue
                    ossValue = self.oss.uploadVideoUrl(item['url'])
                    item['content']  = ossValue.split(' ')[0]
                    item['size'] = ('%.2f' + "MB") % (int(ossValue.split(' ')[1])/math.pow(1024, 2))
                    duration = int(ossValue.split(' ')[2])
                    minutes = str(duration / 60) if duration / 60 >= 10 else '0' + str(duration / 60)
                    seconds = str(duration % 60) if duration % 60 >= 10 else '0' + str(duration % 60)
                    item['duration'] = minutes + ':' + seconds
                    if item['content'] == '':
                        continue
                    item['comment_url'] = str(data['photo_id'])
                    item['original_time'] = data['time']
                    item['status'] = 0
                except Exception, e:
                    print str(e)
                    continue
                add_count(self)
                print str(count) + '.' + item['source_detail'] + ' ' + ': ' + item['url']
                result = self.insert_video(item)
                if result:
                    self.insert_comment(result['_id'], item['comment_url'], result['db'])


video_kuaishou = Video_kuaishou()
video_kuaishou.get_video()
