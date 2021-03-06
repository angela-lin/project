import RPi.GPIO as GPIO
import time, datetime
import sys
from hx711 import HX711
import statistics as stat
from bs4 import BeautifulSoup
import requests
import csv
import redis

def cleanAndExit():
    print("Cleaning...")
    GPIO.cleanup()
    print("Bye!")
    sys.exit()

#Place the offset
def setup():
    hx.set_offset(8449420.3125)
    hx.set_scale(-1031.228)

class QueueTeam:
    def __init__(self, list_of_len = 10):
        self.list_of_len = list_of_len

    def remove_end(self, operatingList):
        if len(operatingList) != 0:
            return operatingList[1:]
        else:
            raise LookupError('The Queueing is empty!')

    def insert_head(self, operatingList, val):
        if len(operatingList) < self.list_of_len:
            operatingList.append(val)
            return operatingList
        else:
            raise LookupError('The Queueing is full!')

    def queueingList(self, operatingList, val):
        operatingList = self.insert_head(self.remove_end(operatingList), val)
        return operatingList

def outLierOrNot(unstable_status, check_list):
    if unstable_status == 3:
        OutLierCandidate = check_list[7:10]
        #print("正在確認是否為異常數值,,,")
        if stat.stdev(OutLierCandidate) > 15:
            del check_list[7]
            check_list.insert(7, old_weight)
            #print("異常值已刪除!")
            stable_status = 0
    return check_list


key = ""
def whichProduct(difference, key):
    if 40 <= abs(difference) < 100:
        key = "1"
    elif 101 <= abs(difference) < 195:
        key = "2"
    elif 196 <= abs(difference) < 232:
        key = "3"
    elif 233 <= abs(difference) < 1000:
        key = "4"
    return key


##################################

if __name__ == "__main__":
    hx = HX711(5, 6)
    qu = QueueTeam(10)
    setup()
    # 貨架擺滿商品的原始重量
    time.sleep(3)
    W = hx.get_grams()
    # 標準差變化判斷值
    STD_CONSTANT = 16
    check_list = [W for i in range(10)]
    test_list = [W for i in range(10)]

    # 個狀態參數
    stable_status = 2
    unstable_status = 0
    mean_weight = stat.mean(check_list)
    old_weight = stat.mean(check_list)
    new_weight = stat.mean(check_list)
    weight_diff = 0
    p1_val = 0
    p2_val = 0
    p3_val = 0
    p4_val = 0
    value = 0

    while True:
        try:
            check_list = qu.queueingList(check_list, hx.get_segment_grams())
            dt = datetime.datetime.now()
            test_list = [dt, hx.get_segment_grams()]
            std = stat.stdev(check_list)
            #print(check_list)
            # 若標準差過大 -> 啟動不穩定狀態，判斷是否為離群值 -> unstable_status開始跑
            if std > STD_CONSTANT:
                unstable_status += 1
                check_list = outLierOrNot(unstable_status, check_list)
            else:
                unstable_status = 0

            # 不穩定狀態 -> 將 stable_status 歸零
            if std > STD_CONSTANT:
                #print('[不穩定狀態]', '標準差為 ', std)
                stable_status = 0
                if unstable_status == 1:
                    old_weight = stat.mean(check_list[0:5])

            # 恢復穩定狀態 -> 將 unstable_status 歸零
            if std <= STD_CONSTANT:
                #print('[穩定狀態]', '標準差為 ', std)
                unstable_status = 0
                if stable_status == 1:
                    new_weight = stat.mean(check_list[5:10])
                    # 此時計算新舊重量差
                    weight_diff = new_weight - old_weight
                    #print("重量相差:", weight_diff)
                    #將此重量差比對商品編號
                    topicKey = whichProduct(weight_diff, key)
                    # 找topic value
                    if weight_diff > 0:
                        if topicKey == "1":
                            p1_val = p1_val - 1
                            value = p1_val
                        elif topicKey == "2":
                            p2_val = p2_val - 1
                            value = p2_val
                        elif topicKey == "3":
                            p3_val = p3_val - 1
                            value = p3_val
                        else:
                            p4_val = p4_val - 1
                            value = p4_val
                        topicValue = str(value)
                        # print("放回商品:編號", topicKey, "消費數量:", topicValue)
                    else:
                        if topicKey == "1":
                            p1_val = p1_val + 1
                            value = p1_val
                        elif topicKey == "2":
                            p2_val = p2_val + 1
                            value = p2_val
                        elif topicKey == "3":
                            p3_val = p3_val + 1
                            value = p3_val
                        else:
                            p4_val = p4_val + 1
                            value = p4_val
                        topicValue = str(value)
                        # print("拿取商品:編號", topicKey, "消費數量:", topicValue)
                    if abs(weight_diff) >= 30:
                        # 上傳redis
                        # redis 集群在那裡?
                        # r = redis.StrictRedis(host='10.120.14.114', port=6379, db=0)
                        r = redis.StrictRedis(host='34.73.182.54', port=6379, db=0, password='*********')
                        name = BeautifulSoup(requests.get('https://0da3c1a2.ngrok.io/welcom').text,
                                             'html.parser').select_one('div')
                        topic_name = name.text
                        # print("topic name = ", topic_name)
                        try:
                            print("Start sending messages ...")
                            # 產生要發佈到Redis的訊息
                            r.hset(topic_name, topicKey, topicValue)
                            print("mainKey =", topic_name, "sub key =", topicKey, "sub value = ", topicValue)
                            print("Message sending completed!")

                        except Exception as e:
                            e_type, e_value, e_traceback = sys.exc_info()
                            print("type ==> %s" % (e_type))
                            print("value ==> %s" % (e_value))
                            print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
                            print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
                            print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))
                    else:
                        print("Nothing has changed.")
                    #print('取放商品完畢，差值為 ' + str(weight_diff))
            with open("Demo_0314.csv", "a") as fp:
                wr = csv.writer(fp, dialect='excel')
                wr.writerow(test_list)
            stable_status += 1
        except (KeyboardInterrupt, SystemExit):
            cleanAndExit()