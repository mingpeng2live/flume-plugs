 #!/bin/bash
 #--------------------------------------------------------------------
 #作者：ming.peng
 #日期：2016-12-12
 #参数：start|stop|restart
 #功能：flume 启动停止重启
 #-------------------------------------------------------------------

path=$(cd `dirname $0`; pwd)
echo $path

JAR="flume"

function start(){
    echo "开始启动 ...."
    source /etc/profile
    num=`ps -ef|grep java|grep $JAR|wc -l`
    echo "进程数:$num"
    if [ "$num" = "0" ] ; then
       #eval nohup java -Xmx512m -jar -DplanNames=$planNames -DconfigPath=$CONFIG_PATH $jarpath/$JAR `echo $@|cut -d " " -f3-$#` >> /dev/null 2>&1 &
       eval nohup flume-ng agent -c $path/conf -f $path/conf/http-test.conf -n agent -Dflume.root.logger=INFO,LOGFILE -Dflume.log.dir=$path/logs -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 >> /dev/null 2>&1 &
       echo "启动成功...."
       echo "日志路径: $path/logs/flume.log"
    else
       echo "进程已经存在，启动失败，请检查....."
       exit 0
    fi
}

function stop(){
    echo "开始stop ....."
    num=`ps -ef|grep java|grep $JAR|wc -l`
    if [ "$num" != "0" ] ; then
     #ps -ef|grep java|grep $JAR|awk '{print $2;}'|xargs kill -9
     # 正常停止flume
     ps -ef|grep java|grep $JAR|awk '{print $2;}'|xargs kill
     echo "进程已经关闭..."
    else
     echo "服务未启动，无需停止..."
    fi
}


function restart(){
    echo "begin stop process ..."
    stop
    # 判断程序是否彻底停止
    num=`ps -ef|grep java|grep $JAR|wc -l`
    while [ $num -gt 0 ]; do
        sleep 1
        num=`ps -ef|grep java|grep $JAR|wc -l`
    done
    echo "process stoped,and starting ..."
    start
    echo "started ..."
}

case "$1" in
    "start")
      start $@
      exit 0
    ;;
    "stop")
      stop
      exit 0
     ;;
    "restart")
       restart
       exit 0
     ;;
    *)
       echo "用法： $0 {start|stop|restart}"
       exit 1
    ;;
esac

