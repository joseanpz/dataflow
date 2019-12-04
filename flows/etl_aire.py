import json
import requests
from time import sleep

import pandas as pd
import dask.dataframe as dd
from prefect import task, Flow
import random
from thingspeak import Channel


MONTERREY = (25.500189438288984, -99.95239163987878)

@task
def get_sensores(latitud, longitud):
    print('***LOG***: Obteniendo ids de los sensores')
    url_sensores = f"https://www.purpleair.com/data.json?opt=1/mAQI/a10/cC0&fetch=true&nwlat=25.79740058873962&selat={latitud}&nwlng=-100.6538006400072&selng={longitud}&fields=pm_1"
    json_response = requests.get(url_sensores).json()
    sensores = [info[0] for info in json_response['data']]
    print('***LOG***: ids OK')
    return sensores


@task
def get_thingspeak_keys(sensores: list):
    print('***LOG***: Obteniendo config de los sensores')
    json_sensores = requests\
        .get('https://www.purpleair.com/json')\
        .json()

    info = []

    for sensor in json_sensores['results']:
        if sensor['ID'] in sensores:
            info.append({
                'ID': sensor['ID'],
                'THINGSPEAK_PRIMARY_ID': sensor['THINGSPEAK_PRIMARY_ID'],
                'THINGSPEAK_PRIMARY_ID_READ_KEY': sensor['THINGSPEAK_PRIMARY_ID_READ_KEY']
            })
        
    print('***LOG***: Config OK ({} sensores)'.format(len(info)))
    return info


@task
def get_sensor_data(
        thingspeak_data, 
        start='2019-10-01', 
        end='2019-10-31', 
        average='daily', 
        timezone='America/Monterrey'):
    print('***LOG***: Obteniendo datos')
    channel = Channel(
        id=thingspeak_data['THINGSPEAK_PRIMARY_ID'], 
        api_key=thingspeak_data['THINGSPEAK_PRIMARY_ID_READ_KEY'])

    raw_data = channel.get_field(
        field='field8',
        options={
            'start': start,
            'end': end,
            'average': average,
            'timezone': timezone
        })

    data = json.loads(raw_data)
    df = pd.DataFrame(data['feeds'])
    serie = pd.Series(
        df['S16791'].values, 
        index= df['hora'].values, 
        name=thingspeak_data['ID'])
    
    print('***LOG***: Datos OK')
    return serie



@task
def reduce_sensor_data(sensor_data):
    ## Usar reduccion
    pass



with Flow("dask-example-aire") as flow:
    sensores = get_sensores(
        latitud=MONTERREY[0], 
        longitud=MONTERREY[1])
    llaves_sensores = get_thingspeak_keys(sensores=sensores)
    sensor_data = get_sensor_data.map(llaves_sensores)
    #data = reduce_sensor_data(sensor_data)


