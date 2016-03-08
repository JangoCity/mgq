# Introduction

Memcached Go Queue, 简称mgq, 是一个用[Go](https://golang.org)语言写的，基于memcached协议的消息队列。其父亲[mcq](https://github.com/stvchu/memcacheq.git)是最早应用于[weibo](http://weibo.com)的基础消息中间件，有着高性能，解耦的优点，使得其广泛应用于微博





# Features


mgq是一个基于NOSQL数据库[BerkeleyDB](http://www.oracle.com/technetwork/cn/database/database-technologies/berkeleydb/overview/index.html)写的FIFO消息队列，目前支持的特性如下：

* 一写多读：举个例子，set myqueue 'one message'，只要get的时候，myqueue开头，#分隔，如myqueue#1，多个客户端之间读是彼此独立的，是不受影响的
* 默认的get是读取队列中未读取的最旧消息
* 支持getc操作，支持获取队列中某一个cursor位置的数据：举个例子，假设myqueue已经有1000条数据，getc myqueue 99,就可以获取队列当中cursor为99的消息
* 支持getr操作，支持获取队列中某一个start cursor位置开始，到end的数据：举个例子，假设myqueue已经有20条数据，getr myqueue 1 10 ,就可以获取队列当中cursor为[1-10]的消息 「内测中」

* getn支持timeout机制的阻塞api来获取队列中的最新消息，举个例子：getn queue 10,意味着10s内有数据则立马返回，否则会10s后立马返回数据不存在的错误，默认getn的timeout是0s，永不超时（需要注意的是如果客户端有getn的操作，则set的另一个客户端需要调用setn）
# Installation


* 首先要安装BerkeleyDB，以版本6.1.26为例，下载[db-6.1.26.tar.gz](http://www.oracle.com/technetwork/cn/database/database-technologies/berkeleydb/downloads/index.html),执行tar -zxvf db-6.1.26.tar.gz;cd db-6.1.26;dist/configure;make && make install
* git clone https://github.com/YoungPioneers/mgq ;cd mgq;make
* cd bin;./mgq 默认端口为22201，可通过./mgq -h查看更多帮助


^_^

# Usage

查看队列的统计情况

stats命令

```
telnet localhost 22201
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
stats
STAT q 0/0
STAT queue 122389/80000
END
```

set 队列名（q为队列名，按下\r\n后输入的'helloworld'为消息内容）

```
set q
helloworld
STORED
```

get 队列名

```
get q
VALUE q 0 10
helloworld
END
```

setn 队列名 

```
setn q
helloworld
STORED
```

getn 队列名 timeout_second

```
getn q 5
VALUE q 0 10
helloworld
END
```

getc 队列名 cursor

```
getc q 9
VALUE q 0 11
helloworld9
END
```

delete 队列名

```
delete queue
DELETED
```



more example in [examples](https://github.com/YoungPioneers/mgq/examples)
