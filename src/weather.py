#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

import os
import sys
import re
import datetime
import json
import yaml
import time
from typing import NamedTuple
import logging
import numbers
import atexit
import paho.mqtt.client as mqtt
import requests


class MqttWeather():
    config_file = 'config.yml'
    mqtt_server_ip = 'localhost'
    mqtt_server_port = 1883
    mqtt_server_user = ''
    mqtt_server_password = ''
    mqtt_base_topic = ''
    owm_api_key = ''
    latitude = ''
    longitude = ''
    api_url = r'https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&units=metric&exclude=alerts,daily,minutely&lang=en&appid={appid}'
    update_freq = 60*15
    
    def __init__(self):
        logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s;<%(levelname)s>;%(message)s')
        logging.info('Init')

        if len(sys.argv) > 1:
            self.config_file = sys.argv[1]

        self.load_config()
        
        #MQTT init
        self.mqttclient = mqtt.Client()
        self.mqttclient.on_connect = self.mqtt_on_connect
        self.mqttclient.enable_logger(logging.getLogger(__name__))

        #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['mqtt_base_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'owm_api_key', 'latitude', 'longitude']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        self.state_topic = self.mqtt_base_topic + '/state'

    def start(self):
        logging.info('starting')

        #MQTT startup
        logging.info('Starting MQTT client')
        self.mqttclient.username_pw_set(self.mqtt_server_user, password=self.mqtt_server_password)
        self.mqttclient.connect(self.mqtt_server_ip, self.mqtt_server_port, 60)
        logging.info('MQTT client started')

        self.mqttclient.loop_start()

        time.sleep(2)
        while True:
            try:
                r = requests.get(self.api_url.format(lat=self.latitude, lon=self.longitude, appid=self.owm_api_key))
                data = r.json()

                payload = dict((k, data['current'][k]) for k in ['temp', 'pressure', 'humidity', 'clouds', 'wind_speed'] 
                                        if k in data['current'])
                payload['temperature'] = payload.pop('temp')
                self.mqttclient.publish(self.mqtt_base_topic+'/current', payload=json.dumps(payload), qos=0, retain=True)

                now = datetime.datetime.now()
                for i in range(1,13):
                    pred_time = now + datetime.timedelta(hours=i)

                    pred = None
                    for hour in data['hourly']:
                        dt = datetime.datetime.fromtimestamp(hour['dt'])
                        if abs(dt - pred_time) <= datetime.timedelta(minutes=30):
                            pred = hour
                            break
                    
                    if pred:
                        payload = dict((k, pred[k]) for k in ['temp', 'pressure', 'humidity', 'clouds', 'wind_speed'] 
                                                if k in pred)
                        payload['temperature'] = payload.pop('temp')
                        self.mqttclient.publish(self.mqtt_base_topic+'/forecast/{}h'.format(i), payload=json.dumps(payload), qos=0, retain=True)

            except Exception as e:
                logging.error(e)

            time.sleep(self.update_freq)

    def programend(self):
        logging.info('stopping')

        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        try:
            logging.info('MQTT client connected with result code: '+mqtt.connack_string(rc))

            self.mqttclient.publish(self.mqtt_base_topic+'/bridge/state', payload='{"state": "online"}', qos=1, retain=True)
            self.mqttclient.will_set(self.mqtt_base_topic+'/bridge/state', payload='{"state": "offline"}', qos=1, retain=True)
        except Exception as e:
            logging.error('Encountered error in mqtt connect handler: '+str(e))

if __name__ == '__main__':
    mqttWeather =  MqttWeather()
    mqttWeather.start()
