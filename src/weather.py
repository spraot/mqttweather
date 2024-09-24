#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge
This script receives MQTT data and saves those to InfluxDB.
"""

import os
import sys
import re
from datetime import datetime, timedelta, timezone
from tzlocal import get_localzone
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
    latitude = ''
    longitude = ''
    altitude = '0'
    api_url = r'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={lat}&lon={lon}&altitude={alt}'
    update_freq = 60*10

    prop_map = {
        'temperature': 'air_temperature',
        'pressure': 'air_pressure_at_sea_level',
        'humidity': 'relative_humidity',
        'clouds': 'cloud_area_fraction',
        'wind_speed': 'wind_speed',
        'wind_direction': 'wind_from_direction',
        'ultraviolet_index_clear_sky': 'ultraviolet_index_clear_sky',
    }
    
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
        self.mqttclient.will_set(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)

        #Register program end event
        atexit.register(self.programend)

        logging.info('init done')

    def load_config(self):
        logging.info('Reading config from '+self.config_file)

        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        for key in ['mqtt_base_topic', 'mqtt_server_ip', 'mqtt_server_port', 'mqtt_server_user', 'mqtt_server_password', 'altitude', 'latitude', 'longitude']:
            try:
                self.__setattr__(key, config[key])
            except KeyError:
                pass

        self.state_topic = self.mqtt_base_topic + '/bridge/state'

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
                headers = {'user-agent': 'mqttweather'}
                r = requests.get(self.api_url.format(lat=self.latitude, lon=self.longitude, alt=self.altitude), headers=headers)
                data = r.json()

                now = datetime.now(timezone.utc)

                data = [
                    {
                        'time': datetime.fromisoformat(x['time']), 
                        **x['data']['instant']['details'],
                        **(x['data']['next_1_hours']['details'] if 'next_1_hours' in x['data'] else {}),
                    }
                    for x in data['properties']['timeseries']]

                for i in range(0,19):
                    pred_time = now + timedelta(hours=i)

                    pred = None
                    for a, b in zip(data[:-1], data[1:]):
                        if a['time'] < pred_time < b['time']:
                            # Interpolate to find exact forecast for pred_time
                            pred = {k: (a[v] + (b[v]-a[v])*(pred_time-a['time'])/(b['time']-a['time'])) for k, v in self.prop_map.items() if v in a and v in b}
                            # Significant digits, round to nearest 0.1
                            pred = {k: round(v*10)/10 for k, v in pred.items()}
                            break
                    
                    if pred:
                        topic = self.mqtt_base_topic+('/current' if i == 0 else '/forecast/{}h'.format(i))
                        self.mqttclient.publish(topic, payload=json.dumps(pred), qos=0, retain=True)

                # Aggregate forecasts for today and for tomorrow
                now_local = now.astimezone(get_localzone())
                end_of_day = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=get_localzone()) + timedelta(days=1)
                for s, e, title in (now, end_of_day, 'today'), (end_of_day, end_of_day + timedelta(days=1), 'tomorrow'):
                    pred_range = [x for x in data if s <= x['time'] < e]
                    if len(pred_range) == 0:
                        logging.error('No prediction data for '+title)
                        continue
                    pred = {
                        'temperature_minimum': min(x[self.prop_map['temperature']] for x in pred_range),
                        'temperature_maximum': max(x[self.prop_map['temperature']] for x in pred_range),
                        'ultraviolet_index_actual_average': sum(0.01*(100-x[self.prop_map['clouds']])*x['ultraviolet_index_clear_sky'] for x in pred_range)/len(pred_range),
                        'wind_speed_max': max(x['wind_speed'] for x in pred_range),
                        'precipitation_amount': sum(x['precipitation_amount'] for x in pred_range if 'precipitation_amount' in x),
                    }
                    pred['ultraviolet_index_actual_average'] = round(pred['ultraviolet_index_actual_average']*100)/100
                    topic = self.mqtt_base_topic+'/forecast/'+title
                    self.mqttclient.publish(topic, payload=json.dumps(pred), qos=0, retain=True)

            except Exception as e:
                logging.error(e)

            time.sleep(self.update_freq)

    def programend(self):
        logging.info('stopping')

        self.mqttclient.publish(self.state_topic, payload='{"state": "offline"}', qos=1, retain=True)
        self.mqttclient.disconnect()
        time.sleep(0.5)
        logging.info('stopped')

    def mqtt_on_connect(self, client, userdata, flags, rc):
        try:
            logging.info('MQTT client connected with result code: '+mqtt.connack_string(rc))

            self.mqttclient.publish(self.state_topic, payload='{"state": "online"}', qos=1, retain=True)
        except Exception as e:
            logging.error('Encountered error in mqtt connect handler: '+str(e))

if __name__ == '__main__':
    mqttWeather =  MqttWeather()
    mqttWeather.start()
