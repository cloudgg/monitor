#!/bin/env python
#-*- coding:utf-8 -*-

import json
import time
import socket
import os
import re
import sys
import commands
import urllib2, base64
import fileinput

#使用到nagios的check_tcp插件
check_tcp = "/home/falcon/agent/nagios/libexec/check_tcp"
redis_cli = commands.getoutput('ls /home/work/redis*/bin/redis-cli').split('\n')[0]
_cpu_regex = re.compile(ur'\n\s*\d+\s+\w+\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(\d+\.?\d*)\s*')
_port_regex = re.compile(ur'TCP (OK) -*')
class RedisStats:
    # 如果你是自己编译部署到redis，请将下面的值替换为你到redis-cli路径
    #_redis_cli = '/home/work/redis-*/bin/redis-cli'
    _redis_cli = redis_cli
    _stat_regex = re.compile(ur'(\w+):([0-9]+\.?[0-9]*)\r')

    def __init__(self,  port='6379', passwd=None, host='127.0.0.1'):
        self._cmd = '%s -h %s -p %s info' % (self._redis_cli, host, port)
        if passwd not in ['', None]:
            self._cmd = "%s -h %s -p %s -a %s info" % (self._redis_cli, host, port, passwd )

    def stats(self):
        ' Return a dict containing redis stats '
        info = commands.getoutput(self._cmd)
        return dict(self._stat_regex.findall(info))

class RedisConfs:
    # 如果你是自己编译部署到redis，请将下面的值替换为你到redis-cli路径
    _redis_cli = redis_cli
    _stat_regex = re.compile(ur'(\w+)\n([0-9]+\.?[0-9]*)')
#    _stat_regex = re.compile(ur'"(\w+)"\r')

    def __init__(self,  port='6379', passwd=None, host='127.0.0.1', **keys):
        self._cmd = '%s -h %s -p %s config get' % (self._redis_cli, host, port)
        self.keys = keys
        if passwd not in ['', None]:
            self._cmd = "%s -h %s -p %s -a %s config get" % (self._redis_cli, host, port, passwd )

    def stats(self):
        ' Return a dict containing redis stats '
        conf_key = {}
        for key,value in self.keys.items():
            self._newcmd = "%s %s" % (self._cmd, key)
            v = commands.getoutput(self._newcmd)
            conf_key[key] = v.split('\n')[1]
        return conf_key


def main():
    ip = socket.gethostname()
    timestamp = int(time.time())
    step = 30
    # inst_list中保存了redis配置文件列表，程序将从这些配置中读取port和password，建议使用动态发现的方法获得，如：
    insts_list = [ i for i in commands.getoutput("find /home/work/redis* -name 'master_[0-9]*.conf'" ).split('\n') ]
    # insts_list = [ '/etc/redis.conf' ]
    p = []

    monit_keys = [
        ('connected_clients','GAUGE'),
        ('blocked_clients','GAUGE'),
        ('used_memory','GAUGE'),
        ('used_memory_rss','GAUGE'),
        ('mem_fragmentation_ratio','GAUGE'),
        ('total_commands_processed','COUNTER'),
        ('rejected_connections','COUNTER'),
        ('instantaneous_ops_per_sec','COUNTER'),
        ('instantaneous_input_kbps','COUNTER'),
        ('instantaneous_output_kbps','COUNTER'),
        ('expired_keys','COUNTER'),
        ('evicted_keys','COUNTER'),
        ('keyspace_hits','COUNTER'),
        ('keyspace_misses','COUNTER'),
        ('keyspace_hit_ratio','GAUGE'),
        ('maxmemory','GAUGE'),
        ('maxclients','COUNTER'),
        ('memory_used_ratio','GAUGE'),
        ('connected_used_ratio','GAUGE'),
        ('proc_cpu','GAUGE'),
        ('port_listen','GAUGE'),
    ]

    for inst in insts_list:
        new_keys = {}
        port = commands.getoutput("sed -n 's/^port *\([0-9]\{4,5\}\)/\\1/p' %s" % inst)
        passwd = commands.getoutput("sed -n 's/^requirepass *\([^ ]*\)/\\1/p' %s" % inst)
        metric = "redis"
        endpoint = ip
        tags = 'port=%s' % port
        mondata_path = "/home/falcon/opbin/redis/data"
        mondata_file = "%s/%s_%s" % (mondata_path,'redis',port)

        try:
            conn = RedisStats(port, passwd)
            infos = conn.stats()
        except Exception,e:
            continue

        try:
            conn = RedisConfs(port, passwd, maxmemory=1, maxclients=2)
            confs = conn.stats()
        except Exception,e:
            continue

        stats = dict(infos, **confs)
        # get cpuinfo
        cmd = """ps aux | grep redis-server|grep %s|grep -v grep |awk '{print $2}'""" % port
        pid = os.popen(cmd).read()
        cmd = "top -bn3 -d0.1 -p %s" % pid
        topinfo = os.popen(cmd).read()
        cpuinfo = _cpu_regex.findall(topinfo)
        stats['proc_cpu'] = cpuinfo[1]

        #check port alive
        cmd = "%s -H 127.0.0.1 -p %s" % (check_tcp, port)
        port_status = os.popen(cmd).read()
        if _port_regex.match(port_status):
            port_status = 1
        else:
            port_status = 0
        stats['port_listen'] = port_status

        new_keys['keyspace_hits'] = stats['keyspace_hits']
        new_keys['keyspace_misses'] = stats['keyspace_misses']
        if not os.path.exists(mondata_path):
            os.makedirs(mondata_path)
        if not os.path.isfile(mondata_file):
            try:
                fn = open(mondata_file,'w+')
                json.dump(new_keys,fn)
                fn.close()
            except Exception,e:
                continue
        try:
            fn = open(mondata_file)
            for line in fn.readlines():
                last_keys = json.loads(line)
                fn.close()
        except Exception,e:
            continue

        try:
            fn = open(mondata_file,'w+')
            for line in fn.readlines():
                last_keys = json.loads(line)
            json.dump(new_keys,fn)
            fn.close()
        except Exception,e:
            continue
       # finally:
       #     fn.close()

        for key,vtype in monit_keys:
            if key == 'keyspace_hit_ratio':
                try:
                    value = (float(stats['keyspace_hits'])) - float(last_keys['keyspace_hits'])/(int(stats['keyspace_hits']) - int(last_keys['keyspace_hits']) + int(stats['keyspace_misses']) - int(last_keys['keyspace_misses']))
                    value = float('%0.2f'%value)
                except ZeroDivisionError:
                    value = 0
            elif key == 'memory_used_ratio':
                try:
                    value = (float(int(stats['used_memory'])))/(int(stats['maxmemory']))
                    value = float('%0.2f'%value)
                except ZeroDivisionError:
                    value = 0
            elif key == 'mem_fragmentation_ratio':
                value = float(stats[key])
            elif key == 'proc_cpu':
                value = float(stats[key])
            elif key == 'connected_used_ratio':
                value = float(stats['connected_clients'])/int(stats['maxclients'])
                value = float('%0.2f'%value)
            else:
                try:
                    value = int(stats[key])
                except:
                    continue

            i = {
                'Metric': '%s.%s' % (metric, key),
                'Endpoint': endpoint,
                'Timestamp': timestamp,
                'Step': step,
                'Value': value,
                'CounterType': vtype,
                'TAGS': tags
            }
            p.append(i)


    print json.dumps(p, sort_keys=True,indent=4)
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    url = 'http://127.0.0.1:1988/v1/push'
    request = urllib2.Request(url, data=json.dumps(p) )
    request.add_header("Content-Type",'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError,e:
        connection = e

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        print connection.read()
    else:
        print '{"err":1,"msg":"%s"}' % connection
if __name__ == '__main__':
    proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    if int(proc) < 5:
        main()
