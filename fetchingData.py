import threading

import json
import requests

from kafkaProducer import producer


def getdata():
    threading.Timer(30.0, getdata).start()
    url = requests.get("https://api.nomics.com/v1/currencies/ticker?key=690b086552758cf1e34cbcba2890ae568f822625")
    text = url.text
    data = json.loads(text)
    producer.send('crypto', data)
    print(data[0])