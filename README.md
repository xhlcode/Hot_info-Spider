# Hot_info-Spider
抓取微信热门文章以及快手大V视频

运行程序：<br/>
1.更改代码中的redis、oss、mongodb的账号密码<br/>
2.运行：<br/>
命令1: python article_wechat.py    功能：抓取清博数据文章日榜（含地区榜单）：http://www.gsdata.cn/rank/wxarc <br/>
命令2: python gf_article.py        功能：抓取清博自定义榜单（只抓取官方榜单）：http://www.gsdata.cn/custom/wxrank <br/>
命令3: python v_kuaishou.py        功能：抓取快手大V（暂定100w粉丝为大V标准）信息 <br/>
命令4: python v_video_kuaishou.py  功能：根据命令3抓取的大V信息抓取大V近一个月内发布的视频 <br/>

由于时间问题，代码写的不好看，有些冗余代码，待日后完善
