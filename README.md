zoosh
=====

ZooKeeper shell with:
   * command history and completion (readline)
   * standard commands like ls, cp, rm, chmod


Version 0.0.1
  * initial version


Sample session
````
INFO  zookeeper connected
Got a new id: 100bd5f53520000
>>  ls
  zookeeper
>>  touch foo
>>  ls
  zookeeper
  foo
>>  ls -lfrz
0000 1969/12/31-16:00:00  v:000 cv:-002 eph:0   /zookeeper
  cdrwa  world::anyone  
0000 1969/12/31-16:00:00  v:000 cv:000 eph:0   /zookeeper/config
  cdrwa  world::anyone  
0000 1969/12/31-16:00:00  v:000 cv:000 eph:0   /zookeeper/quota
  cdrwa  world::anyone  
0000 2016/01/13-16:56:18  v:000 cv:000 eph:0   /foo
  cdrwa  world::anyone  
>>  
>>  stat /zookeeper/
[/zookeeper]:
	ctime = Wed Dec 31 16:00:00 1969
	czxid=0
	mtime=Wed Dec 31 16:00:00 1969
	mzxid=0
	version=0	aversion=0
	ephemeralOwner = 0
>>  
?      cat    cd     chmod  cp     exit   help   ls     pwd    quit   rm     stat   touch  
>>  rm foo
>>  ls
  zookeeper
>>  

