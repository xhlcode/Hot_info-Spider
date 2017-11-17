# -*- coding: utf-8 -*-
import oss2
import time
import requests
import random
from moviepy.editor import VideoFileClip
from bs4 import BeautifulSoup
import sys
import os

class Oss_Adapter:
    _bucket      = None
    video_size   = ''
    cur_schedule = 0

    def __init__(self):
        auth         = oss2.Auth('xxx', 'xxx')
        service      = oss2.Service(auth, 'xxx')
        self._bucket = oss2.Bucket(auth,  'xxx', 'xxx')
        pass

    def uploadPage(self, data, name):
        ret = self._bucket.put_object(name, data)
        if ret.status == 200:
            return True
        return false
        pass

    def uploadImageFile(self, fileobj):
        name = self.getName('image') + '.jpg'
        ret = self._bucket.put_object(name, fileobj)
        if ret.status == 200:
            return name
        return ''
        pass

    def uploadImageData(self, data, name):
        ret = self._bucket.put_object(name, data)
        if ret.status == 200:
            return True
        return false
        pass

    def uploadImageUrl(self, url, refer='', name='', tp=''):
        if not url:
            return ''
        if tp:
            suffix = '.' + tp
        else:
            suffix  = url.split('/')[-1]
            if -1   ==  suffix.find('.'):
                suffix = '.jpg'
            else:
                suffix  = '.' + suffix.split('.')[-1]
            suffix  = suffix.split('?')[0]
            if suffix == '.cache':
                suffix = '.jpg'
            if suffix not in ['.jpg','.jpeg','.png','.gif']:
                return ''
        if suffix == '.None' or suffix == '.other':
            suffix = '.jpg'
        headers = {'User-Agent' : 'Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 4 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19'}
        if refer !='':
            headers['Referer'] = refer
        try:
            input   = requests.get(url, headers=headers, timeout = 1)
        except:
            print 'timeout url: ' + url
        if name == '':
            name = self.getName('image') + suffix
        ret    = self._bucket.put_object(name, input)
        if ret.status == 200:
            return name
        return ''
        pass

    def percentage(self, consumed_bytes, total_bytes):
        # 进度条回调函数，计算当前完成的百分比
        rate = int(100 * (float(consumed_bytes) / float(self.video_size)))
        if self.cur_schedule < rate:
            self.cur_schedule = rate
            print '\r{0}% '.format(self.cur_schedule)

        sys.stdout.flush()

    def uploadVideoUrl(self, url, refer='', name=''):
        print 'upload url: ' + url
        if not url:
            return ''
        headers = {'User-Agent' : 'Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 4 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19'}
        if refer !='':
            headers['Referer'] = refer
        try:
            input = requests.get(url, headers=headers, timeout = 2)
        except:
            print 'timeout url'
            return ''
        if name == '':
            name = self.getName('video') + '.mp4'
        try:
            self.video_size = input.headers['Content-length']
            print 'video_size: ' + self.video_size
            ret = self._bucket.put_object(name, input, progress_callback=self.percentage)
        except Exception, e:
            print str(e)
            return ''
        duration = 0
        if refer not in ['iu.snssdk.com']:
            try:
                with open(name, 'w') as f:
                    f.write(input.content)
                clip = VideoFileClip(name)
                duration = int(clip.duration)
            finally:
                os.remove(name)
        self.cur_schedule = 0
        if ret.status == 200:
            return name + ' ' + self.video_size + ' ' + str(duration)
        return ''
        pass

    def delPage(self, name):
        pass

    def checkExists(self, name):
        pass

    def delImage(self, name):
        self._bucket.delete_object(name)
        pass

    def delVideo(self, name):
        self._bucket.delete_object(name)
        pass

    def getName(self, type):
        key = {'image':'afjei()83#$!', 'page':'fjeiw!#@179', 'video':'xsjw!#072&'}
        import hashlib
        m   = hashlib.md5()
        m.update(key[type] + str(time.time()) + type + str(random.random()))
        return m.hexdigest()

    def dealContent(self, content, refer):
        soup = BeautifulSoup(content, 'html.parser')
        for tag in  soup.find_all('img'):
            if tag.has_attr('src'):
               url = tag['src']
            else:
               url = tag['data-src']
            url            = self.uploadImageUrl(url, refer)
            new_tag        = soup.new_tag("img")
            new_tag['src'] = "/pic/" + url
            tag.replace_with(new_tag)
        print soup.prettify()
