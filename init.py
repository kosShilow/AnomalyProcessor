#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import json
import subprocess
import time
import logging
from datetime import datetime
import logging
import os
import math
from pyxmpp2.simple import send_message
import re

# def get_anomaly_period(timeseries_anomaly):
#     # levels = {}
#     # # timeseries_anomaly_compressed = []
#     # # val_prev = -1
#     # for t_v in timeseries_anomaly:
#     #     if t_v[1] > 0:
#     #         levels[t_v[1]] = t_v[1]
#     #     # if (val_prev == 0 and t_v[1] != 0) or (val_prev != 0 and t_v[1] == 0) or (val_prev != 0 and t_v[1] != 0):
#     #     #     mas = [t_v[0], t_v[1]]
#     #     #     timeseries_anomaly_compressed.append(mas)
#     #     # val_prev = t_v[1]
#     #
#     # anomaly = {}
#     # for level in levels:
#     #     anomaly_level = get_anomaly_period_level(timeseries_anomaly, level)
#     #     # anomaly[level] = anomaly_level
#
#     anomaly_level = get_anomaly_period_level(timeseries_anomaly)
#     return anomaly_level

def get_anomaly_period_level(timeseries_anomaly):
    anomaly_period = []
    inside = False
    x_min = 0
    x_max = 0
    level = 0
    x = 0
    for t_v in timeseries_anomaly:
        if inside and t_v[1] == 0:
            inside = False
            if x_max != 0:
                mas = []
                mas.append(x_min)
                mas.append(x_max)
                mas.append(level)
                anomaly_period.append(mas)
            x_min = 0
            x_max = 0
            level = 0
        if t_v[1] > 0:
            if not inside:
                x_min=t_v[0]
                x_max = t_v[0]
                inside = True
            else:
                x_max = t_v[0]
            level = level + t_v[1] * math.exp(-(x*x)/100)
        x = x + 1
    if x_min != 0 and x_max != 0:
        mas = []
        mas.append(x_min)
        mas.append(x_max)
        mas.append(level)
        anomaly_period.append(mas)
    return anomaly_period

def get_uplinks():
    f = open('\\\\10.120.63.3/eee/neb.map', 'r', encoding="utf8")
    data = f.read()
    info = json.loads(data)

    links = info.get("area_chermk").get("links")
    sw_uplink = {}
    for link in links:
        node_iface = link[0]+":"+link[2]
        sw_uplink[node_iface] = node_iface
        node_iface = link[3]+":"+link[5]
        sw_uplink[node_iface] = node_iface
    return sw_uplink

def nodes_ifaces_anomaly_logging(log_file, start_time, stop_time, comparison=">", porog=0, porog_uplink=0):
    with open("node-iface-anomaly.out", 'r') as f:
        nodes_ifaces_anomaly = json.loads(f.read())
        # print("len="+str(len(nodes_ifaces_anomaly)))
    f.close()

    msgs = []
    for node in nodes_ifaces_anomaly:
        # ifaces = {}
        for iface in nodes_ifaces_anomaly[node]:
            # flag = False
            if sw_uplink.get(node+":"+iface):
                porog = porog_uplink
            for prefix in nodes_ifaces_anomaly[node][iface]:
                # flag1 = False
                start_stop_list = nodes_ifaces_anomaly[node][iface][prefix]
                if comparison == ">":
                    for start_stop in start_stop_list:
                        start = start_stop[0]
                        stop = start_stop[1]
                        level = start_stop[2]
                        if level > porog and stop - start > cfg["anomaly-duration"]:
                            if (start > start_time and start < stop_time and stop > start_time and stop < stop_time) or (
                                    start < start_time and stop > start_time and stop < stop_time) or (
                                    start > start_time and start < stop_time and stop > stop_time) or (
                                    start < start_time and stop > stop_time):
                                # print(node+":"+iface+":"+prefix+":"+str(int(lv))+":"+str(int(time.time())))
                                msgs.append(node + ":" + iface + ":" + prefix + ":" + str(level))
                                break
                elif comparison == ">=":
                    for start_stop in start_stop_list:
                        start = start_stop[0]
                        stop = start_stop[1]
                        level = start_stop[2]
                        if level > porog and stop - start > cfg["anomaly-duration"]:
                            if (start > start_time and start < stop_time and stop > start_time and stop < stop_time) or (
                                    start < start_time and stop > start_time and stop < stop_time) or (
                                    start > start_time and start < stop_time and stop > stop_time) or (
                                    start < start_time and stop > stop_time):
                                # print(node+":"+iface+":"+prefix+":"+str(int(lv))+":"+str(int(time.time())))
                                msgs.append(node + ":" + iface + ":" + prefix + ":" + str(level))
                                break
                elif comparison == "==":
                    for start_stop in start_stop_list:
                        start = start_stop[0]
                        stop = start_stop[1]
                        level = start_stop[2]
                        if level > porog and stop - start > cfg["anomaly-duration"]:
                            if (start > start_time and start < stop_time and stop > start_time and stop < stop_time) or (
                                    start < start_time and stop > start_time and stop < stop_time) or (
                                    start > start_time and start < stop_time and stop > stop_time) or (
                                    start < start_time and stop > stop_time):
                                # print(node+":"+iface+":"+prefix+":"+str(int(lv))+":"+str(int(time.time())))
                                msgs.append(node + ":" + iface + ":" + prefix + ":" + str(level))
                                break
                elif comparison == "<":
                    for start_stop in start_stop_list:
                        start = start_stop[0]
                        stop = start_stop[1]
                        level = start_stop[2]
                        if level > porog and stop - start > cfg["anomaly-duration"]:
                            if (start > start_time and start < stop_time and stop > start_time and stop < stop_time) or (
                                    start < start_time and stop > start_time and stop < stop_time) or (
                                    start > start_time and start < stop_time and stop > stop_time) or (
                                    start < start_time and stop > stop_time):
                                # print(node+":"+iface+":"+prefix+":"+str(int(lv))+":"+str(int(time.time())))
                                # write_anomaly_msg(log_file, node + ":" + iface + ":" + prefix + ":" + str(
                                #     int(lv)) + ":" + str(int(time.time())), anomaly_log_file, timeout)
                                msgs.append(node + ":" + iface + ":" + prefix + ":" + str(level))
                                break
                elif comparison == "<=":
                    for start_stop in start_stop_list:
                        start = start_stop[0]
                        stop = start_stop[1]
                        level = start_stop[2]
                        if level > porog and stop - start > cfg["anomaly-duration"]:
                            if (start > start_time and start < stop_time and stop > start_time and stop < stop_time) or (
                                    start < start_time and stop > start_time and stop < stop_time) or (
                                    start > start_time and start < stop_time and stop > stop_time) or (
                                    start < start_time and stop > stop_time):
                                # print(node+":"+iface+":"+prefix+":"+str(int(lv))+":"+str(int(time.time())))
                                msgs.append(node + ":" + iface + ":" + prefix + ":" + str(level))
                                break
    write_anomaly_msgs(log_file, msgs, 60*60*24)



def write_anomaly_msgs(log_file, msgs, timeout):
    # your_jid = "noc@10.120.63.10"
    # your_password = "1qaz2wsx"
    your_jid = "admin@10.120.63.10"
    your_password = "123456"
    target_jid = "noc@10.120.63.10"

    # remove old records from log_file
    lines = []
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            for line in f:
                line = line
                mas1 = line.split(" - ")
                if len(mas1) == 2:
                    mas2 = mas1[1].rstrip().split(":")
                    if len(mas2) == 6:
                        timestamp = int(mas2[4])
                        if time.time() - timeout < timestamp:
                            lines.append(line)
                        else:
                            print('Delete old record - '+line.rstrip())
                            logging.debug('Delete old record - '+line.rstrip())
        f.close()

    f = open(log_file, "w")
    for line in lines:
        f.write(line)
    f.close


    node_iface_prefix_level = {}
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            lines = []
            for line in f:
                lines.append(line)
            lines = lines[-1::-1]

            exclude_lines = {}
            for line in lines:
                mas1 = line.split(" - ")
                if len(mas1) == 2:
                    mas2 = mas1[1].rstrip().split(":")
                    if len(mas2) == 6:
                        if mas2[5] == "anomaly":
                            if not node_iface_prefix_level.get(mas2[0] + ":" + mas2[1] + ":" + mas2[2]) and not exclude_lines.get(
                                    mas2[0] + ":" + mas2[1] + ":" + mas2[2]):
                                node_iface_prefix_level[mas2[0] + ":" + mas2[1] + ":" + mas2[2]] = mas2[0] + ":" + mas2[1] + ":" + mas2[2] + ":" + mas2[3]
                        else:
                            exclude_lines[mas2[0]+":"+mas2[1]+":"+mas2[2]] = mas2[0]+":"+mas2[1]+":"+mas2[2] + ":" + mas2[3]

        f.close()

    f = open(log_file, "a")
    f_log = open(log_file.split(".")[0]+".log", "a")
    for msg in msgs:
        mas = msg.split(":")
        if not node_iface_prefix_level.get(mas[0] + ":" + mas[1] + ":" + mas[2]) and not exclude_lines.get(mas[0] + ":" + mas[1] + ":" + mas[2]):
            node_iface_prefix_level[mas[0] + ":" + mas[1] + ":" + mas[2]] = mas[0] + ":" + mas[1] + ":" + mas[2] + ":" + mas[3]
            f.write(str(datetime.today())+" - "+msg+":"+str(int(time.time()))+":anomaly\n")
            f_log.write(str(datetime.today()) + " - " + msg + ":" + str(int(time.time())) + ":anomaly\n")
            print(msg+":anomaly")
            if re.search(r".+_in", mas[2]):
                send_message(your_jid, your_password, target_jid, "node: "+mas[0]+"     iface: "+mas[1]+"     metrics: "+mas[2]+"     "+mas[3]+"  -  anomaly ---> http://10.120.63.1:9999/"+mas[0]+"/"+mas[1].replace("/", "|"))

    for prev_msg in node_iface_prefix_level:
        find = False
        for msg in msgs:
            mas = msg.split(":")
            if mas[0] + ":" + mas[1] + ":" + mas[2] == prev_msg:
                find = True
                break
        if not find:
            f.write(str(datetime.today())+" - "+node_iface_prefix_level[prev_msg]+":"+str(int(time.time()))+":OK\n")
            f_log.write(str(datetime.today()) + " - " + node_iface_prefix_level[prev_msg] + ":" + str(int(time.time())) + ":OK\n")
            print(prev_msg+":OK")
            mmm = prev_msg.split(":")
            if re.search(r".+_in", mmm[2]):
                send_message(your_jid, your_password, target_jid, "node: "+mmm[0]+"     iface: "+mmm[1]+"     metrics: "+mmm[2]+"  -  OK ---> http://10.120.63.1:9999/"+mmm[0]+"/"+mmm[1].replace("/", "|"))
    f.close()
    f_log.close()
#################################################################################
logging.basicConfig(filename='AnomalyProcessor.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

while True:
    # datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Start AnomalyProcessor.')
    logging.info('Start AnomalyProcessor.')
    time_start = time.time()
    with open('AnomalyProcessor.cfg') as json_file:
        cfg = json.load(json_file)

    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    r = redis.Redis(connection_pool=pool)

    #################################################
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Start KeyList.')
    logging.debug('Start KeyList.')

    num_process = cfg["num_process"]
    f = []
    i = 0
    while i < num_process:
        f.append(open(str(i)+".in", 'w'))
        i = i + 1

    node_iface = {}
    i = 0
    for key in r.scan_iter():
        # print("key="+str(key))
        val = str(key).split("'")[1]
        mas = val.split(":")
        if len(mas) == 3 and mas[2] != "ifSpeed":
            f[i].write(val + '\n')
            i = i + 1
            if i == num_process:
                i = 0
            if mas[2] == "bits_in":
                ifaces = node_iface.get(mas[0])
                if ifaces:
                    ifaces[mas[1]] = ""
                else:
                    ifaces = {}
                    ifaces[mas[1]] = ""
                    node_iface[mas[0]] = ifaces

    i = 0
    while i < num_process:
        f[i].close()
        i = i + 1
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Stop KeyList.')
    logging.debug('Stop KeyList.')

    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Start NodeIfaceAnomaly list.')
    logging.debug('Start NodeIfaceAnomaly list.')
    node_iface_anomaly = {}
    for node in node_iface:
        ifaces = {}
        for iface in node_iface.get(node):
            prefixs = ["bits_in", "bits_out", "ucast_in", "ucast_out", "broadcast_in", "broadcast_out",
                       "multicast_in", "multicast_out", "errors_in", "errors_out", "discards_in", "discards_out",
                       "unknown_protocols_out"]

            prefix_anomaly = {}
            for prefix in prefixs:
                if r.exists(node + ":" + iface + ":" + prefix + ":anomaly"):
                    timeseries_anomaly = r.lrange(node + ":" + iface + ":" + prefix + ":anomaly", 0, -1)
                    # prefix_anomaly = {}
                    if len(timeseries_anomaly):
                        time_val_series = []
                        for time_val in timeseries_anomaly:
                            mas = str(time_val).split("'")[1].split("|")
                            t_v = []
                            t_v.append(float(mas[0]))
                            t_v.append(float(mas[1]))
                            time_val_series.append(t_v)
                        level_anomaly_period = get_anomaly_period_level(time_val_series)
                        if len(level_anomaly_period):
                            prefix_anomaly[prefix] = level_anomaly_period
            ifaces[iface] = prefix_anomaly

        if len(ifaces):
            node_iface_anomaly[node] = ifaces
        # print("node="+node)

    pool.disconnect()

    json_file = open("node-iface-anomaly.out", "w")
    a = json.dumps(node_iface_anomaly)
    json_file.write(a)
    json_file.close()

    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Stop NodeIfaceAnomaly list.')
    logging.debug('Stop NodeIfaceAnomaly list.')

    # print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S") + ': Start anomaly logging.')
    # logging.debug('Start anomaly logging.')
    # start_time = time.time() - 60 * 60
    # stop_time = time.time()
    # nodes_ifaces_anomaly_logging("anomaly.out", start_time, stop_time, comparison=">", porog=50)
    # print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S") + ': Stop anomaly logging.')
    # logging.debug('Stop anomaly logging.')

    ####################################
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Running anomaly daemons.')
    logging.debug('Running anomaly daemons.')

    program = "python AnomalyProcessor.py"

    i = 0
    process = []
    while i < num_process:
        process.append(subprocess.Popen(program+" "+str(i)+".in"))
        i = i + 1

    i = 0
    while i < num_process:
        process[i].wait()
        i = i + 1

    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Stop anomaly daemons.')
    logging.debug('Stop anomaly daemons.')

    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S") + ': Start anomaly logging.')
    sw_uplink = get_uplinks()
    logging.debug('Start anomaly logging.')
    start_time = time.time() - 60 * 60
    stop_time = time.time()
    nodes_ifaces_anomaly_logging("anomaly.out", start_time, stop_time, comparison=">", porog=0.1, porog_uplink=1)
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S") + ': Stop anomaly logging.')
    logging.debug('Stop anomaly logging.')

    logging.info('Stop AnomalyProcessor.')
    logging.info('####################################################################')
    print(datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S")+': Stop AnomalyProcessor.')
    print("#####################################################")

    time_stop = time.time()
    pause_time = cfg["timeout"]-(time_stop-time_start)
    if pause_time <= 0:
        pause_time = 0
    print("pause="+str(pause_time)+" sec.")
    time.sleep(pause_time)
