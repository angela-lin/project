from kafka import KafkaConsumer
import requests
from bs4 import BeautifulSoup
import sys

if __name__ == "__main__":
    # 設定要連線到Kafka集群的相關設定, 產生一個Kafka的Consumer的實例
    consumer = KafkaConsumer(
        # Kafka集群在那裡?
        bootstrap_servers=["10.120.14.114:9092"],
        # 指定msgKey的反序列化器, 若Key為None, 無法反序列化
        key_deserializer=bytes.decode,
        # 指定msgValue的反序列化器
        value_deserializer=bytes.decode,
        # 是否從這個ConsumerGroup尚未讀取的partition / offset開始讀
        auto_offset_reset="latest",
    )

    # 步驟2.指定想要訂閱訊息的topic名稱
    topic_name = BeautifulSoup(requests.get('https://0da3c1a2.ngrok.io/welcom').text, 'html.parser').select_one('div').text

    # 步驟3.讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe(topics=topic_name)

    # 步驟4.持續的拉取Kafka有進來的訊息
    try:
        print("Start listen incoming messages ...")
        # 持續監控是否有新的record進來
        for record in consumer:
            topic = record.topic
            partition = record.partition
            offset = record.offset
            timestamp = record.timestamp
            # 取出msgKey與msgValue
            msgKey = record.key
            msgValue = record.value
            # 秀出metadata與msgKey & msgValue訊息
            print("topic=%s, partition=%s, offset=%s : (key=%s, value=%s), time=%s" % (record.topic, record.partition,
                                                                              record.offset, record.key, record.value, timestamp))
    except:
        # 錯誤處理
        e_type, e_value, e_traceback = sys.exc_info()
        print("type ==> %s" % (e_type))
        print("value ==> %s" % (e_value))
        print("traceback ==> file name: %s" % (e_traceback.tb_frame.f_code.co_filename))
        print("traceback ==> line no: %s" % (e_traceback.tb_lineno))
        print("traceback ==> function name: %s" % (e_traceback.tb_frame.f_code.co_name))

    finally:
        #關閉連線
        consumer.close()