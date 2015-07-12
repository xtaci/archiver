# arch(归档)
[![Build Status](https://travis-ci.org/GameGophers/arch.svg?branch=master)](https://travis-ci.org/GameGophers/arch)

# 设计思路
对游戏中通过nsq-redo包发送过来的变动数据，纪录游戏中所有的变动，每隔一段时间，会产生一个带有时间标记的新的RDO文件, 格式为: REDO-2006-01-02T15:04:05.RDO，暂定的归档文件轮替时间为24小时

## 使用
参考测试用例以

## 安装
参考Dockerfile

# 环境变量
> NSQD_HOST: eg : http://172.17.42.1:4151         
> NSQLOOKUPD_HOST: eg: http://127.0.0.1:4161         
