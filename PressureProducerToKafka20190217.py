import RPi.GPIO as GPIO
import time
import sys
from hx711 import HX711
from kafka import KafkaProducer

# Force Python 3 ###########################################################

if sys.version_info[0] != 3:
    raise Exception("Python 3 is required.")

############################################################################


hx = HX711(5, 6)

# 給定kafka value 初始值 0
product1_val = 0
product2_val = 0
product3_val = 0
product4_val = 0

val_list = []



def cleanAndExit():
    print("Cleaning...")
    GPIO.cleanup()
    print("Bye!")
    sys.exit()

# Place the offset
def setup():
    hx.set_offset(8133650.0469)
    hx.set_scale(-1192.4775)


def listOfValue(val_list, val):
    val_list.append(val)
    return val_list


# def loop(val_list):
#     """
#     code run continuosly
#     """



##################################

if __name__ == "__main__":
    #設定要連線到Kafka集群的相關設定, 並產生一個Kafka的Producer的實例
    producer = KafkaProducer(
        # Kafka集群在那裡?
        bootstrap_servers=["10.120.14.114:9092"],
        # 指定msgKey的序列化器, 若Key為None, 無法序列化, 透過producer直接給值
        key_serializer=str.encode,
        # 指定msgValue的序列化器
        value_serializer=str.encode
    )

#開啟應力計偵測
    setup()
    print("If you want to exit, please use ctrl+c.")
    time.sleep(0.5)
    while True:
        try:
            val = hx.get_grams()
            val_list = listOfValue(val_list, val)
            print(val)
            #        hx.power_down()
            time.sleep(1)

            # delete old value
            if len(val_list) >= 20:
                del val_list[0:len(val_list) - 5]

            # determineChange
            if len(val_list) >= 5:
                valuesChange = abs(val_list[2] - val_list[0])
                valuesDiffer = abs(val_list[3] - val_list[0])
                Differ = {
                    0 <= valuesDiffer < 65: "product1",
                    66 <= valuesDiffer < 115: "product2",
                    116 <= valuesDiffer < 165: "product3",
                    166 <= valuesDiffer < 1000: "product4",
                }

                # Given the key-value dictionary
                key_value = {
                    "product1": product1_val,
                    "product2": product2_val,
                    "product3": product3_val,
                    "product4": product4_val
                }

                if valuesChange > 30 and val_list[2] < val_list[0]:
                    print("There are some action.")
                    print("The product is taken:", Differ[1])

                    #            指定想要發佈訊息的topic名稱
                    topic_name = "test"
                    topicKey = str(Differ[1])

                    # 如果 key_value[1] = product1_val，product1_val = product1_val+1
                    key_value[0] = topicKey
                    if key_value[0] == "product1":
                        product1_val = product1_val + 1

                    if key_value[0] == "product2":
                        product2_val = product2_val + 1

                    if key_value[0] == "product3":
                        product3_val = product3_val + 1

                    if key_value[0] == "product4":
                        product4_val = product4_val + 1

                    topicValue = str(key_value[1])
                    print("topic_value = ", topicValue)

                    msg_counter = 0
                    try:
                        print("Start sending messages ...")
                        # 產生要發佈到Kafka的訊息
                        # - 參數  # 1: topicName
                        # - 參數  # 2: msgKey
                        # - 參數  # 3: msgValue
                        producer.send(topic=topic_name, key=topicKey, value=topicValue)
                        msg_counter += 1
                        print("Send " + str(msg_counter) + " messages to Kafka")
                        print("Message sending completed!")

                        # delete old value
                        if len(val_list) >= 20:
                            del val_list[0:len(val_list) - 5]



                    except Exception as e:
                        # 錯誤處理
                        e_type, e_value, e_traceback = sys.exc_info()
                        print("type ==> %s" % (e_type))
                        print("value ==> %s" % (e_value))
                        print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
                        print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
                        print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))



                elif valuesChange > 30 and val_list[2] > val_list[0]:
                    print("There are some action.")
                    print("The product is putting back:", Differ[1])
                    #            指定想要發佈訊息的topic名稱
                    topic_name = "test"
                    topicKey = Differ[1]
                    # 如果 key_value[1] = product1_val，product1_val = product1_val11
                    key_value[0] = topicKey
                    if key_value[0] == "product1":
                        product1_val = product1_val - 1

                    if key_value[0] == "product2":
                        product2_val = product2_val - 1

                    if key_value[0] == "product3":
                        product3_val = product3_val - 1

                    if key_value[0] == "product4":
                        product4_val = product4_val - 1
                    topicValue = str(key_value[1])
                    print("topicValue = ", topicValue)

                    msg_counter = 0
                    try:
                        print("Start sending messages ...")
                        # 產生要發佈到Kafka的訊息
                        # - 參數  # 1: topicName
                        # - 參數  # 2: msgKey
                        # - 參數  # 3: msgValue
                        producer.send(topic=topic_name, key=topicKey, value=topicValue)
                        msg_counter += 1
                        print("Send " + str(msg_counter) + " messages to Kafka")
                        print("Message sending completed!")

                        # delete old value
                        if len(val_list) >= 20:
                            del val_list[0:len(val_list) - 5]

                    except Exception as e:
                        # 錯誤處理
                        e_type, e_value, e_traceback = sys.exc_info()
                        print("type ==> %s" % (e_type))
                        print("value ==> %s" % (e_value))
                        print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
                        print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
                        print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))


        except (KeyboardInterrupt, SystemExit):
            # GPIO.cleanup()
            # hx.power_down()
            cleanAndExit()

            # 關掉Producer實例的連線
            producer.close()
