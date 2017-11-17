# -*- coding: utf-8 -*-
import redis
import hashlib

class CheckExist:
    _redis = None
    _type = None

    def __init__(self, _type):
        self._type = _type
        self._redis = redis.StrictRedis(host='localhost', port=6379,db=2, password='xxx')

    def md5Encode(self, str):
        m = hashlib.md5()
        m.update(str)
        return m.hexdigest()

    def checkExist(self, str):
        md5 = self.md5Encode(str)
        result = self._redis.zscore(self._type + '_unique_timer', md5)
        if not result:
            return True
        return False
