#!/usr/bin/python

import matplotlib.pyplot as plt
import redis
import datetime
import time

import os
import time
import datetime
import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
# import sys
# import rrdtool
import json
import numpy

# from threading import Thread, Event
from wsrpc import WebSocketRoute, WebSocketThreaded as WebSocket, wsrpc_static

def cb():
    GraphsHandler.sw_uplink = get_uplinks()

def counters(timeseries):
    counters_series = []
    time_prev = -1
    time_prev_prev = -1
    val_prev = -1
    val_prev_prev = -1
    # i = 0
    for t_v in timeseries:
        # if i == 105:
        #     print("111")
        time = t_v[0]
        val = t_v[1]
        if time_prev != -1:
            res = val - val_prev
            if res >= 0:
                if val_prev_prev == -1:
                    mas = []
                    mas.append(time)
                    mas.append((val - val_prev) / (time - time_prev))
                    counters_series.append(mas)
                else:
                    if val >= val_prev_prev:
                        mas = []
                        mas.append(time)
                        mas.append((val - val_prev_prev) / (time - time_prev_prev))
                        counters_series.append(mas)
                    else:
                        mas = []
                        mas.append(time)
                        mas.append((val - val_prev) / (time - time_prev))
                        counters_series.append(mas)
                time_prev_prev = -1
                val_prev_prev = -1
            else:
                time_prev_prev = time_prev
                val_prev_prev = val_prev
                # print("i="+str(i))
        time_prev = time
        val_prev = val
        # i = i + 1
    return counters_series

def get_timeseries(r, key, type = "COUNTERS"):
    timeseries = r.lrange(key, 0, -1)
    time_val_series = []
    for time_val in timeseries:
        mas = str(time_val).split("'")[1].split("|")
        t_v = []
        t_v.append(float(mas[0]))
        t_v.append(float(mas[1]))
        time_val_series.append(t_v)
    if type == "COUNTERS":
        time_val_series = counters(time_val_series)

    return time_val_series

def get_anomaly_period(timeseries_anomaly, duration):
    anomaly_period = []
    inside = False
    x_min = 0
    x_max = 0
    for t_v in timeseries_anomaly:
        if inside and t_v[1] == 0:
            inside = False
            if x_max != 0 and x_max - x_min > duration:
                mas = []
                mas.append(x_min)
                mas.append(x_max)
                anomaly_period.append(mas)
            x_min = 0
            x_max = 0
        if t_v[1] > 0:
            if not inside:
                x_min=t_v[0]
                # x_max = t_v[0]
                inside = True
            else:
                x_max=t_v[0]
    if x_min != 0 and x_max != 0 and x_max - x_min > duration:
        mas = []
        mas.append(x_min)
        mas.append(x_max)
        anomaly_period.append(mas)
    return anomaly_period

def insert_nan_in_timeseries(timeseries, delta):
    timeseries_new = []
    if timeseries:
        timestamp_prev = timeseries[0][0]
        for t_v in timeseries:
            if t_v[0] - timestamp_prev > delta:
                timeseries_new.append([t_v[0], numpy.nan])
            timeseries_new.append([t_v[0], t_v[1]])
            timestamp_prev = t_v[0]
    return timeseries_new

def get_period(timeseries, start, stop):
    timeseries = insert_nan_in_timeseries(timeseries, cfg["timeout"]*2)
    out = []
    for t_v in timeseries:
        if t_v[0] > start and t_v[0] < stop:
            out.append([t_v[0], t_v[1]])
    return out

def diagram(r, key, start, stop, xlabels_period, image_file):
    # if key == "10.124.248.34:GigabitEthernet4/40:discards_out":
    #     print(key)
    # pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    # r = redis.Redis(connection_pool=pool)
    timeseries = get_period(get_timeseries(r, key, type="COUNTERS"), start, stop)
    timeseries_predicted = get_period(get_timeseries(r, key+":predicted", type="GAUGE"), start, stop)
    timeseries_lower = get_period(get_timeseries(r, key+":lower", type="GAUGE"), start, stop)
    timeseries_upper = get_period(get_timeseries(r, key+":upper", type="GAUGE"), start, stop)
    timeseries_anomaly = get_period(get_timeseries(r, key + ":anomaly", type="GAUGE"), start, stop)
    # pool.disconnect()

    anomaly_period = get_anomaly_period(timeseries_anomaly, cfg["anomaly-duration"])

    x = []
    y = []
    for t_v in timeseries:
        x.append(t_v[0])
        y.append(t_v[1])
    x_predicted = []
    y_predicted = []
    for t_v in timeseries_predicted:
        x_predicted.append(t_v[0])
        y_predicted.append(t_v[1])
    x_lower = []
    y_lower = []
    for t_v in timeseries_lower:
        x_lower.append(t_v[0])
        y_lower.append(t_v[1])
    x_upper = []
    y_upper = []
    for t_v in timeseries_upper:
        x_upper.append(t_v[0])
        y_upper.append(t_v[1])
    anomaly = []
    for t_v in timeseries_anomaly:
        anomaly.append(t_v[1])

    # fig = plt.figure()
    # ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    plt.ioff()
    fig, ax = plt.subplots(figsize=(7, 3))
    if x or y:
        ax.plot(x, y, color='blue', linewidth=3, label=': Value')
    if x_predicted or y_predicted:
        ax.plot(x_predicted, y_predicted, color='#ff00ff', label=': Forecast')
    if x_lower or y_lower:
        ax.plot(x_lower, y_lower, color='green', label=': Lower')
    if x_upper or y_upper:
        ax.plot(x_upper, y_upper, color='red', label=': Upper')
    ax.set_ylabel(key.split(":")[2]+"/sec.")
    ax.grid(True)
    ax.legend()
    ax.set_ylim(0, None)
    xax = ax.xaxis
    xlocs = xax.get_ticklocs()
    yax = ax.yaxis
    ylocs = yax.get_ticklocs()

    start = xlocs[0]
    stop = xlocs[-1]
    # delta = stop - start
    xticks = []
    xlabels = []
    sec = start
    while sec <= stop:
        t = datetime.datetime.fromtimestamp(sec)
        if t.hour == 0 and t.minute == 0 and t.second == 0:
            xticks.append(sec)
            # xlabels.append(t.strftime("%d/%m/%y")+" 00:00")
            xlabels.append(t.strftime("%d/%m"))
        elif t.hour % xlabels_period == 0 and t.minute == 0 and t.second == 0:
            xticks.append(sec)
            hour = t.strftime("%H")
            if hour[0] == "0":
                hour = hour[1]
            xlabels.append(hour+":00")
        sec = sec + 1
    ax.set_xticks(xticks)
    ax.set_xticklabels(xlabels)

    ylabels = []
    for count in ylocs:
        if count/1000000000000 >= 1:
            ylabels.append(str(count/1000000000000)+" T")
        elif count/1000000000 >= 1 and count/1000000000 < 1000:
            ylabels.append(str(count/1000000000)+" G")
        elif count/1000000 >= 1 and count/1000000 < 1000:
            ylabels.append(str(count/1000000)+" M")
        elif count/1000 >= 1 and count/1000 < 1000:
            ylabels.append(str(count/1000)+" K")
        else:
            ylabels.append(str(round(count, 3)))
    ax.set_yticklabels(ylabels)
    # ax.set_ylim(0, None)

    fig.tight_layout()

    for min_max in anomaly_period:
        plt.axvspan(min_max[0], min_max[1], facecolor='#fdd017', alpha=1.0)

    # plt.show()
    plt.savefig(image_file)
    plt.cla()
    plt.clf()
    plt.close(fig)

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

class WSHandler(tornado.websocket.WebSocketHandler):
    # home = ""
    # data_dir = "data/"
    pngpath = 'png/'
    if not os.path.exists(pngpath):
        os.makedirs(pngpath)
    # width = '500'
    # height = '200'
    clients = []
    node = ""
    iface = ""
    sysname = ""


    @tornado.web.asynchronous
    def open(self):
        start_time = time.time() - 60 * 60
        stop_time = time.time()
        out = self.nodes_ifaces_list(start_time, stop_time, True, comparison=">", porog=0.1)
        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.r = redis.Redis(connection_pool=self.pool)

        # print(WSHandler.node+" --- "+WSHandler.iface)
        if WSHandler.node and WSHandler.iface:
            self.write_message(out + ";" + WSHandler.node + ";" + WSHandler.iface)
            pngpath = self.pngpath + self.request.remote_ip + "/"
            self.generate_images(self.r, WSHandler.node, WSHandler.iface, pngpath)
            self.write_message("GRAPHS OK")
        else:
            self.write_message(out)
        self.clients.append(self)


    @tornado.web.asynchronous
    def on_message(self, message):
        # print('message received %s' % message)
        mas1 = message.split(":")
        if mas1[0] == "node_iface":
            mas = mas1[1].split(";")
            WSHandler.node = mas[0]
            WSHandler.iface = mas[1]

            pngpath = self.pngpath + self.request.remote_ip + "/"
            self.generate_images(self.r, WSHandler.node, WSHandler.iface, pngpath)

            self.write_message("GRAPHS OK")
        if mas1[0] == "view_mode":
            view_mode = mas1[1]
            # print "view_mode="+view_mode
            if view_mode == "All":
                start_time = time.time() - 60 * 60
                stop_time = time.time()
                out = self.nodes_ifaces_list(start_time, stop_time, False)
                self.write_message("view_mode_ok:"+out)
                pass
            elif view_mode == "Anomaly":
                start_time = time.time() - 60 * 60
                stop_time = time.time()
                out = self.nodes_ifaces_list(start_time, stop_time, True, comparison=">", porog=0)
                # out = self.nodes_ifaces_list(True)
                self.write_message("view_mode_ok:"+out)
                pass
            elif view_mode == "Critical":
                start_time = time.time() - 60 * 60
                stop_time = time.time()
                out = self.nodes_ifaces_list(start_time, stop_time, True, comparison=">", porog=0.1)
                # out = self.nodes_ifaces_list(True)
                self.write_message("view_mode_ok:"+out)
                pass

    @tornado.web.asynchronous
    def on_close(self):
        # print 'connection closed'
        self.pool.disconnect()
        self.clients.remove(self)
        self.close()

    def generate_images(self, r, node, iface, pngpath):
        val = r.get(node + ":sysname")
        if val:
            WSHandler.sysname = r.get(node + ":sysname")
        # pngpath = self.pngpath + self.request.remote_ip + "/"
        if not os.path.exists(pngpath):
            os.makedirs(pngpath)
        # remove all old png files
        for fpng in os.listdir(pngpath):
            os.remove(pngpath + fpng)
        prefixs = ["bits_in", "bits_out", "ucast_in", "ucast_out", "broadcast_in", "broadcast_out",
                   "multicast_in", "multicast_out", "errors_in", "errors_out", "discards_in", "discards_out",
                   "unknown_protocols_out"]
        for prefix in prefixs:
            xlabels_period = 6  # 6 hour time labels
            key = node + ":" + iface + ":" + prefix
            image_file = pngpath + prefix + ".png"
            diagram(self.r, key, time.time() - 60 * 60 * 24 * 2, time.time(), xlabels_period, image_file)

    def nodes_ifaces_list(self, start_time, stop_time, anomaly=False, comparison=">", porog=0):
        # start_time = time.time() - 60 * 60
        # stop_time = time.time()
        # print("start_time="+str(start_time)+"    stop_time="+str(stop_time))
        # get information from node-iface-anomaly.out
        # start1 = time.time()
        nodes_ifaces_anomaly = {}
        f = open("node-iface-anomaly.out", 'r')
        data = f.read()
        nodes_ifaces_anomaly = json.loads(data)
        # print("len data=" + str(len(data)))
        # print("len=" + str(len(nodes_ifaces_anomaly)))
        f.close()

        # with open("node-iface-anomaly.out", 'r') as f:
        #     nodes_ifaces_anomaly = json.loads(f.read())
        #     # print("len="+str(len(nodes_ifaces_anomaly)))
        # f.close()
        # delta = time.time() - start1
        # print("Read file=" + str(delta))

        # start2 = time.time()
        nodes_ifaces = {}
        if not anomaly:
            for node in nodes_ifaces_anomaly:
                ifaces = {}
                for iface in nodes_ifaces_anomaly[node]:
                    ifaces.update({iface: iface})
                if ifaces:
                    nodes_ifaces.update({node: ifaces})
        else:
            for node in nodes_ifaces_anomaly:
                ifaces = {}
                for iface in nodes_ifaces_anomaly[node]:
                    flag = False
                    for prefix in nodes_ifaces_anomaly[node][iface]:
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
                                        flag = True
                                        # print(node+":"+iface+":"+prefix+" = ["+str(start)+", "+str(stop))
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
                                        flag = True
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
                                        flag = True
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
                                        flag = True
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
                                        flag = True
                                        break
                        if flag:
                            break
                    if flag:
                        ifaces.update({iface: iface})
                if ifaces:
                    nodes_ifaces.update({node: ifaces})

        # delta = time.time() - start2
        # print("Working file=" + str(delta))

        # print("Anomaly len="+str(len(nodes_ifaces)))
        # ooo = ""
        # for node in nodes_ifaces:
        #     ooo = ooo + ", " + node
        # print(ooo)

        node_list = []
        for node in nodes_ifaces:
            node_list.append(node)

        out = "<ul class=\"tree\">"
        node_list.sort()
        for node in node_list:
            ifaces_dict = nodes_ifaces[node]
            ifaces = []
            for iface in ifaces_dict:
                ifaces.append(iface)
            ifaces.sort()
            if ifaces:
                out = out + "<li>" + node + "<ul>"
                for iface in ifaces:
                    out = out + "<li id=\"" + node + "\">" + iface + "</li>"
                out = out + "</ul></li>"
        out = out + "</ul>"

        return out

class BaseHandler(tornado.web.RequestHandler):
    # def prepare(self):
    #     print("request="+str(self.request))
    def get_current_user(self):
        return self.get_secure_cookie("user", max_age_days=0.05)

class BaseHandler1(tornado.web.RequestHandler):
    # def prepare(self):
    #     print("request1="+str(self.request))

    def get_current_user(self):
        WSHandler.node = self.path_args[0]
        WSHandler.iface = self.path_args[1]
        # print("BaseHandler1: "+WSHandler.node+", "+WSHandler.iface)
        return self.get_secure_cookie("user", max_age_days=0.05)

class MainHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        WSHandler.node = ""
        WSHandler.iface = ""
        self.render('html/index.html')

class LoginHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        incorrect = self.get_secure_cookie("incorrect", max_age_days=0.05)
        if incorrect and int(incorrect) > 20:
            self.write('<center>blocked</center>')
            return
        self.render('html/login.html')

    @tornado.gen.coroutine
    def post(self):
        incorrect = self.get_secure_cookie("incorrect", max_age_days=0.05)
        if incorrect and int(incorrect) > 20:
            self.write('<center>blocked</center>')
            return

        getusername = tornado.escape.xhtml_escape(self.get_argument("username"))
        getpassword = tornado.escape.xhtml_escape(self.get_argument("password"))
        if "noc" == getusername and "1qaz2wsx" == getpassword:
            self.set_secure_cookie("user", self.get_argument("username"), expires_days=1)
            self.set_secure_cookie("incorrect", "0", expires_days=1)
            if WSHandler.node and WSHandler.iface:
                # print("reverse_url: " + WSHandler.node + ", " + WSHandler.iface)
                self.redirect(self.reverse_url("nodeiface", WSHandler.node, WSHandler.iface))
            else:
                self.redirect(self.reverse_url("main"))
        else:
            incorrect = self.get_secure_cookie("incorrect", max_age_days=0.05) or 0
            increased = str(int(incorrect)+1)
            self.set_secure_cookie("incorrect", increased, expires_days=1)
            self.write("""<center>
                            Something Wrong With Your Data (%s)<br />
                            <a href="/">Go Home</a>
                          </center>""" % increased)

class LogoutHandler(BaseHandler):
    def get(self):
        self.clear_cookie("user")
        self.redirect(self.get_argument("next", self.reverse_url("main")))

class GraphsHandler(BaseHandler):
    sw_uplink = {}
    def get(self):
        if WSHandler.node and WSHandler.iface:
            uplink = ""
            if GraphsHandler.sw_uplink.get(WSHandler.node+":"+WSHandler.iface):
                uplink = "(uplink)"

            # print("html/graphs.html ip_client="+self.request.remote_ip+", node="+WSHandler.node+", iface="+WSHandler.iface)
            self.render('html/graphs.html', ip_client=self.request.remote_ip, node=WSHandler.node, iface=WSHandler.iface, sysname=WSHandler.sysname, uplink=uplink)

class MyStaticFileHandler(tornado.web.StaticFileHandler):
    def set_extra_headers(self, path):
        # Disable cache
        self.set_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')

class NodeIfaceHandler(BaseHandler1):

    @tornado.web.authenticated
    def get(self, node, iface):
        # print("+++ " + node + ", " + iface)
        WSHandler.node = node
        WSHandler.iface = iface.replace("|", "/")
        # print("--- "+WSHandler.node+", "+WSHandler.iface)
        self.render('html/index.html')

class Unused(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        pass

project_root = os.path.dirname(os.path.abspath(__file__))

settings = {
    "cookie_secret": "bZJc2sWbQLKos6GkHn/VB9oXwQt8S0R0kRvJ5/xJ89E=",
    "login_url": "/login",
    'debug':True,
    "xsrf_cookies": True,
}

application = tornado.web.Application([
    (r'/ws', WSHandler),
    (r'/png/*/*(.*)', MyStaticFileHandler, {
        'path': os.path.join(project_root, 'png'),
    }),
    (r'/(favicon.ico)', MyStaticFileHandler, {"path": "html/image"}),
    tornado.web.url(r"/", MainHandler, name="main"),
    tornado.web.url(r'/login', LoginHandler, name="login"),
    tornado.web.url(r'/logout', LogoutHandler, name="logout"),
    tornado.web.url(r'/graphs', GraphsHandler, name="graphs"),
    tornado.web.url(r"/(\d+\.\d+\.\d+\.\d+)/(graphs)", Unused, name="unused"),
    tornado.web.url(r"/(\d+\.\d+\.\d+\.\d+)/([^/]+)", NodeIfaceHandler, name="nodeiface"),
    # tornado.web.url(r"/(\d+\.\d+\.\d+\.\d+)/((?!graphs).*$)", NodeIfaceHandler, name="nodeiface"),
], **settings)

if __name__ == "__main__":
    with open('AnomalyProcessor.cfg') as json_file:
        cfg = json.load(json_file)
    # diagram("10.96.112.13:GigabitEthernet1/0/19:bits_in", time.time() - 60 * 60 * 24 * 2, time.time(), 6,
    #         "png/10.96.120.39/10.96.112.13$GigabitEthernet1-0-19$bits_in.png")

    GraphsHandler.sw_uplink = get_uplinks()
    port=9999
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)
    tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=7200), WSHandler.on_close)
    period_cbk = tornado.ioloop.PeriodicCallback(cb, 10*60*1000)
    period_cbk.start()
    tornado.ioloop.IOLoop.instance().start()

