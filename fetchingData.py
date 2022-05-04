import json
import requests

from kafkaProducer import producer


def getdatafromurl():
    url = requests.get("https://api.nomics.com/v1/currencies/ticker?key=690b086552758cf1e34cbcba2890ae568f822625")
    text = url.text
    return json.loads(text)
