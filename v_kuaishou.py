# -*- coding: utf-8 -*-
import requests
import datetime
from pymongo import MongoClient
from lxml import html
import hashlib
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

count = 0
def add_count(self):
    global count
    count += 1

class V_kuaishou:
    user_info_url = 'http://123.59.169.36/rest/n/user/profile/v2?appver=5.4.1.284&did=18A8D514-C9F7-4076-9E35-9C5574F9C7AB&c=a&ver=5.4&sys=ios11.1&mod=iPhone6,1&net=中国联通_5'
    headers = {'Host': '123.59.169.36', 'X-REQUESTID': '1500898', 'Accept': 'application/json', 'User-Agent': 'kwai-ios', 'Accept-Language': 'zh-Hans-CN;q=1'}
    post_data = {'client_key': '56c3713c', 'country_code': 'cn', 'exp_tag': '1_a/1583553105481641984_h86', 'language': 'zh-Hans-CN;q=1', 'sig': '', 'user': ''}

    def __init__(self):
        self.client = MongoClient('xxx',xxx)
        self.db = self.client.crawler
        self.db.authenticate('xxx', 'xxx')
        pass

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def get_sig(self, user_id):
        s = 'appver=5.4.1.284c=aclient_key=56c3713ccountry_code=cndid=18A8D514-C9F7-4076-9E35-9C5574F9C7ABexp_tag=1_a/1583553105481641984_h86language=zh-Hans-CN;q=1mod=iPhone6,1net=中国联通_5sys=ios11.1user=1000000ver=5.423caab00356c'
        s = re.sub(r'user=(\d+)', 'user=' + str(user_id), s)
        return self.md5Encode(s)

    def update_V(self):
        author = self.db.author
        user_id = 7263035
        while True:
            try:
                print user_id
                self.post_data['sig'] = self.get_sig(user_id)
                self.post_data['user'] = str(user_id)
                ret = requests.post(self.user_info_url, data=self.post_data)
                data = json.loads(ret.content)
                data = data['userProfile']
                if data['ownerCount']['fan'] > 1000000:
                    item = {}
                    item['icon'] = data['profile']['headurl']
                    item['name'] = data['profile']['user_name']
                    item['intro'] = data['profile']['user_text']
                    item['memo'] = str(data['profile']['user_id'])
                    item['fan_num'] = data['ownerCount']['fan']
                    print str(user_id) + ': ' + str(item['fan_num'])
                    item['channel'] = 'kuaishou'
                    import time
                    item['created_at'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    add_count(self)
                    print str(count) + '.' + item['name'] + ': ' + item['memo'] + '  ' + str(item['fan_num'])
                    try:
                        if not author.find_one({'memo': item['memo']}):
                            author.insert_one(item)
                            print 'insert success'
                        else:
                            print 'exist'
                    except Exception, e:
                        print str(e)
            except Exception, e:
                print str(e)
                print 'error id: ' + str(user_id)
            user_id += 1

v_kuaishou = V_kuaishou()
v_kuaishou.update_V()
