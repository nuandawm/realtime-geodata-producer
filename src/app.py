import numpy as np
import pandas as pd
from kafka import KafkaProducer
from jproperties import Properties
from json import dumps

CONSUMER_CONFIG_KEYS = [
    'bootstrap_servers', 'security_protocol',
    'sasl_mechanism', 'sasl_plain_username', 'sasl_plain_password'
]

value_serializer_callable = lambda x: dumps(x).encode('utf-8')

if __name__ == "__main__":
    configs = Properties()
    with open('config.properties', 'rb') as config_file:
        configs.load(config_file)

    config_dict = {k: v.data for k, v in configs.items() if k in CONSUMER_CONFIG_KEYS}

    producer = KafkaProducer(value_serializer=value_serializer_callable, **config_dict)

    data = {'test': 'puppaaaaa'}

    producer.send('puppa-topic', value=data)

    producer.close()
    producer.flush()

