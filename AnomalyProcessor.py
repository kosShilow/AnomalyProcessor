#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis
import json
import time
import sys
import os
import logging
import re


class HoltWinters:
    """
    Модель Хольта-Винтерса с методом Брутлага для детектирования аномалий
    https://fedcsis.org/proceedings/2012/pliks/118.pdf

    # series - исходный временной ряд
    # slen - длина сезона
    # alpha, beta, gamma - коэффициенты модели Хольта-Винтерса
    # n_preds - горизонт предсказаний
    # scaling_factor - задаёт ширину доверительного интервала по Брутлагу (обычно принимает значения от 2 до 3)

    """

    def __init__(self, series, slen, alpha, beta, gamma, n_preds, scaling_factor=2.96):
        self.series = series
        self.slen = slen
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.n_preds = n_preds
        self.scaling_factor = scaling_factor

    def initial_trend(self):
        sum = 0.0
        for i in range(self.slen):
            sum += float(self.series[i + self.slen] - self.series[i]) / self.slen
        return sum / self.slen

    def initial_seasonal_components(self):
        seasonals = {}
        season_averages = []
        n_seasons = int(len(self.series) / self.slen)
        # вычисляем сезонные средние
        for j in range(n_seasons):
            season_averages.append(sum(self.series[self.slen * j:self.slen * j + self.slen]) / float(self.slen))
        # вычисляем начальные значения
        for i in range(self.slen):
            sum_of_vals_over_avg = 0.0
            for j in range(n_seasons):
                sum_of_vals_over_avg += self.series[self.slen * j + i] - season_averages[j]
            seasonals[i] = sum_of_vals_over_avg / n_seasons
        return seasonals

    def triple_exponential_smoothing(self):
        self.result = []
        self.Smooth = []
        self.Season = []
        self.Trend = []
        self.PredictedDeviation = []
        self.UpperBond = []
        self.LowerBond = []

        seasonals = self.initial_seasonal_components()

        for i in range(len(self.series) + self.n_preds):
            if i == 0:  # инициализируем значения компонент
                smooth = self.series[0]
                trend = self.initial_trend()
                self.result.append(self.series[0])
                self.Smooth.append(smooth)
                self.Trend.append(trend)
                self.Season.append(seasonals[i % self.slen])

                self.PredictedDeviation.append(0)

                self.UpperBond.append(self.result[0] +
                                      self.scaling_factor *
                                      self.PredictedDeviation[0])

                self.LowerBond.append(self.result[0] -
                                      self.scaling_factor *
                                      self.PredictedDeviation[0])

                continue
            if i >= len(self.series):  # прогнозируем
                m = i - len(self.series) + 1
                self.result.append((smooth + m * trend) + seasonals[i % self.slen])

                # во время прогноза с каждым шагом увеличиваем неопределенность
                self.PredictedDeviation.append(self.PredictedDeviation[-1] * 1.01)

            else:
                val = self.series[i]
                last_smooth, smooth = smooth, self.alpha * (val - seasonals[i % self.slen]) + (1 - self.alpha) * (
                        smooth + trend)
                trend = self.beta * (smooth - last_smooth) + (1 - self.beta) * trend
                seasonals[i % self.slen] = self.gamma * (val - smooth) + (1 - self.gamma) * seasonals[i % self.slen]
                self.result.append(smooth + trend + seasonals[i % self.slen])

                # Отклонение рассчитывается в соответствии с алгоритмом Брутлага
                self.PredictedDeviation.append(self.gamma * abs(self.series[i] - self.result[i])
                                               + (1 - self.gamma) * self.PredictedDeviation[-1])

            self.UpperBond.append(self.result[-1] +
                                  self.scaling_factor *
                                  self.PredictedDeviation[-1])

            self.LowerBond.append(self.result[-1] -
                                  self.scaling_factor *
                                  self.PredictedDeviation[-1])

            self.Smooth.append(smooth)
            self.Trend.append(trend)
            self.Season.append(seasonals[i % self.slen])


def Holt_Winters(timeseries, slen, alpha, beta, gamma, n_preds):
    holtwinters = HoltWinters(timeseries[:len(timeseries) - 1], slen, alpha, beta, gamma, n_preds)

    holtwinters.triple_exponential_smoothing()

    result_predicted = holtwinters.result[-1]
    upper = holtwinters.UpperBond[-1]
    lower = holtwinters.LowerBond[-1]
    if lower < 0:
        lower = 0

    return [result_predicted, lower, upper]


def counters(timeseries, ifSpeed):
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
            res_speed = (val - val_prev) / (time - time_prev)
            if res_speed >= 0:
                if val_prev_prev == -1:
                    mas = []
                    mas.append(time)
                    mas.append((val - val_prev) / (time - time_prev))
                    if mas[1] < ifSpeed:
                        counters_series.append(mas)
                else:
                    if val >= val_prev_prev:
                        mas = []
                        mas.append(time)
                        mas.append((val - val_prev_prev) / (time - time_prev_prev))
                        if mas[1] < ifSpeed:
                            counters_series.append(mas)
                    else:
                        mas = []
                        mas.append(time)
                        mas.append((val - val_prev) / (time - time_prev))
                        if mas[1] < ifSpeed:
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

def delete_old_values(key):
    while r.llen(key) > 0:
        val = r.lpop(key)
        mas = str(val).split("'")[1].split("|")
        t_v = [float(mas[0]), float(mas[1])]
        if t_v[0] > float(time.time()) - cfg["retain"]*60*60*24:
            r.lpush(key, val)
            break
        else:
            # print("Delete old value: " + key + " - " + str(val))
            pass
    if r.exists(key) and r.llen(key) == 0:
        r.delete(key)


def write_anomaly(key, timeseries, prefix, type):
    # print(key)
    if type == "COUNTERS":
        fields = key.split(":")
        ifSpeed = 2**64
        if r.get(fields[0]+":"+fields[1] + ":ifSpeed"):
            ifSpeed = int(str(r.get(fields[0]+":"+fields[1] + ":ifSpeed")).split("'")[1])
        time_val_counters = counters(timeseries, ifSpeed)
        timeseries = time_val_counters

    if len(timeseries) > 0:
        timestamp = 0
        if r.get(key + ":timestamp"):
            timestamp = float(str(r.get(key + ":timestamp")).split("'")[1])
        processing_timeseries = []
        if timestamp:
            for t_v in timeseries:
                if t_v[0] > timestamp:
                    mas = [t_v[0], t_v[1]]
                    processing_timeseries.append(mas)
        else:
            mas = [timeseries[-1][0], timeseries[-1][1]]
            processing_timeseries.append(mas)

        for t_v in processing_timeseries:
            cur_time = t_v[0]
            cur_value = t_v[1]

            max_retain_value = 0
            for t_v in timeseries[::-1]:
                if t_v[0] > time.time() - cfg["levels-retain"]:
                    if t_v[1] > max_retain_value:
                        max_retain_value = t_v[1]
                else:
                    break

            if not cfg["levels"].get(prefix + "-min"):
                cfg["levels"][prefix + "-min"] = 0
            if not cfg["levels"].get(prefix + "-critical"):
                cfg["levels"][prefix + "-critical"] = sys.float_info.max
            if not cfg["levels"].get(prefix + "-warning"):
                cfg["levels"][prefix + "-warning"] = cfg["levels"][prefix + "-critical"]

            if cur_value > cfg["levels"][prefix + "-critical"]:
                average = trafic_average(fields[0], fields[1], fields[2])
                traffic_koefficient = average/(1024*1024*1024)
                res = traffic_koefficient * cfg["score"][prefix + "-critical"] * (cur_value - cfg["levels"][prefix + "-critical"]) / \
                      cfg["levels"][prefix + "-critical"]
                r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|"+str(res))
                # print(str(key) + ":anomaly, " + str(int(cur_value)) + "|" + str(res))
                logging.debug(str(key) + ":anomaly, " + str(int(cur_value)) + "|" + str(res))
            elif cur_value > cfg["levels"][prefix + "-warning"]:
                average = trafic_average(fields[0], fields[1], fields[2])
                traffic_koefficient = average/(1024*1024*1024)
                res = traffic_koefficient * cfg["score"][prefix + "-warning"] * (cur_value - cfg["levels"][prefix + "-warning"]) / \
                      cfg["levels"][prefix + "-warning"]
                r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|"+str(res))
                # print(str(key) + ":anomaly, " + str(int(cur_value)) + "|" + str(res))
                logging.debug(str(key) + ":anomaly, " + str(int(cur_value)) + "|" + str(res))
            elif len(timeseries) > cfg["min-anomaly-size"] and max_retain_value > cfg["levels"][
                prefix + "-min"] and max_retain_value < cfg["levels"][prefix + "-warning"]:
                vals = []
                for time_val in timeseries:
                    vals.append(time_val[1])
                a = Holt_Winters(vals, slen, alpha, beta, gamma, n_preds)
                result_predicted = int(a[0])
                lower = int(a[1])
                upper = int(a[2])

                r.rpush(str(key) + ":predicted", str(int(time.time())) + "|" + str(result_predicted))
                r.rpush(str(key) + ":lower", str(int(time.time())) + "|" + str(lower))
                r.rpush(str(key) + ":upper", str(int(time.time())) + "|" + str(upper))
                # print(str(key) + ":predicted", str(int(time.time())) + "|" + str(result_predicted))
                # print(str(key) + ":lower, "+ str(int(time.time())) + "|" + str(lower))
                # print(str(key) + ":upper, "+ str(int(time.time())) + "|" + str(upper))
                if cur_value < lower:
                    res = 0
                    if lower:
                        average = trafic_average(fields[0], fields[1], fields[2])
                        traffic_koefficient = average / (1024 * 1024 * 1024)
                        res = traffic_koefficient * cfg["score"]["holt-winters"] * (lower - cur_value) / lower
                    r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|"+str(res))
                    # print(str(key) + ":anomaly, " + str(int(time.time())) + "|" + str(res))
                    # logging.debug(str(key) + ":anomaly, " + str(int(cur_value)) + "|" + str(res))
                    # pass
                elif cur_value > upper:
                    res = 0
                    if upper:
                        average = trafic_average(fields[0], fields[1], fields[2])
                        traffic_koefficient = average / (1024 * 1024 * 1024)
                        res = traffic_koefficient * cfg["score"]["holt-winters"] * (cur_value - upper) / upper
                    r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|"+str(res))
                    # print(str(key) + ":anomaly(holt-winters), " + str(int(time.time())) + "|" + str(res))
                    logging.debug(str(key) + ":anomaly(holt-winters), " + str(int(cur_value)) + "|" + str(res))
                    # pass
                else:
                    val = r.rpop(str(key) + ":anomaly")
                    if val:
                        mas = str(val).split("'")[1].split("|")
                        if len(mas) == 2:
                            prev_val = float(mas[1])
                            r.rpush(str(key) + ":anomaly", val)
                            if prev_val != 0:
                                r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|0")
            else:
                val = r.rpop(str(key) + ":anomaly")
                if val:
                    mas = str(val).split("'")[1].split("|")
                    if len(mas) == 2:
                        prev_val = float(mas[1])
                        r.rpush(str(key) + ":anomaly", val)
                        if prev_val != 0:
                            r.rpush(str(key) + ":anomaly", str(int(time.time())) + "|0")

            r.set(key + ":timestamp", str(int(cur_time)))
    ###### delete old values from timeseries
    delete_old_values(key)
    delete_old_values(str(key) + ":anomaly")
    delete_old_values(str(key) + ":predicted")
    delete_old_values(str(key) + ":lower")
    delete_old_values(str(key) + ":upper")

def trafic_average(node, iface, prefix):
    timeseries = []
    if node and iface:
        if re.search(r"_in$", prefix):
            len = r.llen(node + ":" + iface + ":bits_in")
            if len > cfg["min-anomaly-size"]:
                timeseries = r.lrange(node + ":" + iface + ":bits_in", len - cfg["min-anomaly-size"], -1)
            else:
                timeseries = r.lrange(node + ":" + iface + ":bits_in", 0, -1)
        elif re.search(r"_out$", prefix):
            len = r.llen(node + ":" + iface + ":bits_out")
            if len > cfg["min-anomaly-size"]:
                timeseries = r.lrange(node + ":" + iface + ":bits_out",
                                      len - cfg["min-anomaly-size"], -1)
            else:
                timeseries = r.lrange(node + ":" + iface + ":bits_out", 0, -1)

    time_val_series = []
    for time_val in timeseries:
        mas = str(time_val).split("'")[1].split("|")
        t_v = []
        t_v.append(float(mas[0]))
        t_v.append(float(mas[1]))
        time_val_series.append(t_v)

    ifSpeed = int(r.get(node+":"+iface+":ifSpeed"))
    timeseries_counters = counters(time_val_series, ifSpeed)
    average = 0
    i = 0
    for time_val in timeseries_counters:
        average = average + time_val[1]
        i = i + 1
    return average/i


###########################################################################################

exclude = "exclude"
filename = sys.argv[1]
# filename = "0.in"
# print(filename)

logging.basicConfig(filename='AnomalyProcessor.log',level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

f = open(filename, "r")
lines = []
for line in f.readlines():
    lines.append(line[:len(line) - 1])
f.close()

os.remove(filename)

f = open(exclude, "r")
exclude_lines = {}
for line in f.readlines():
    exclude_lines[line[:len(line) - 1]] = line[:len(line) - 1]
f.close()

slen = 3
alpha = 0.1
beta = 0.0035
gamma = 0.1
n_preds = 1

with open('AnomalyProcessor.cfg') as json_file:
    cfg = json.load(json_file)

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)

i = 0
for key in lines:
    mas = str(key).split(":")
    if (len(mas) == 3):
        key_type = mas[2]

        if exclude_lines.get(key):
            next
        timeseries = r.lrange(key, 0, -1)
        time_val_series = []
        for time_val in timeseries:
            mas = str(time_val).split("'")[1].split("|")
            t_v = []
            t_v.append(float(mas[0]))
            t_v.append(float(mas[1]))
            time_val_series.append(t_v)

        # if len(time_val_series) > 0 and time_val_series[-1][0] > float(last_timestamp):
        if len(time_val_series) > 0:
            if key_type == "bits_in" or key_type == "bits_out":
                write_anomaly(key, time_val_series, "bits", type="COUNTERS")
            elif key_type == "ucast_in" or key_type == "ucast_out":
                write_anomaly(key, time_val_series, "ucast", type="COUNTERS")
            elif key_type == "broadcast_in" or key_type == "broadcast_out":
                write_anomaly(key, time_val_series, "broadcast", type="COUNTERS")
            elif key_type == "multicast_in" or key_type == "multicast_out":
                write_anomaly(key, time_val_series, "multicast", type="COUNTERS")
            elif key_type == "errors_in" or key_type == "errors_out":
                write_anomaly(key, time_val_series, "errors", type="COUNTERS")
            elif key_type == "discards_in" or key_type == "discards_out":
                write_anomaly(key, time_val_series, "discards", type="COUNTERS")
            elif key_type == "unknown_protocols_out":
                write_anomaly(key, time_val_series, "unknown_protocols", type="COUNTERS")
    i = i + 1

pool.disconnect()
