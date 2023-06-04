import os
import logging
import json
import csv
from multiprocessing import Process
import multiprocessing
import threading
from queue import Queue, Empty
import pandas as pd

from external.client import YandexWeatherAPI
from utils import *


QUEUE_TIMEOUT = 0.01


def ignoreExceptions(ex):
    known = ['HTTP Error 404:', 'Extra data:']
    for s in known:
        if (s in ex.__str__()):
            return True

    return False


class DataFetchingTask(threading.Thread):

    def __init__(self, responses: multiprocessing.Queue, cities: Queue, response_folder: str):
        threading.Thread.__init__(self)
        self.responses = responses
        self.cities = cities
        self.response_folder = response_folder
        self.logger = logging.getLogger()

    def run(self) -> None:
        thread = threading.current_thread()
        name = thread.name
        while True:

            try:
                (city, url) = self.cities.get(timeout=QUEUE_TIMEOUT)
            except Empty as ex:
                self.logger.debug(f'DataFetchingTask: Thread {name} cities (queue) is empty!')
                break

            self.logger.debug(f'Thread {name} started fetching {city}')
            path = f'{self.response_folder}/{city}.json'
            
            try:
                data = json.dumps(YandexWeatherAPI.get_forecasting(url))
            except Exception as ex:
                if ignoreExceptions(ex):
                    self.logger.info(f'Ignored exception: {ex}')
                    continue

                raise Exception(ex)
            
            #Проверка на пустой ответ
            if 'forecasts' not in data:
                self.logger.debug(f'DataFetchingTask: response for {city} gave no data.')
                continue

            with open(path, 'w') as file:
                file.write(data)

            self.responses.put((city, path))
            self.logger.debug(f'Thread {name} wrote in {path}')

        self.logger.debug(f'DataFetchingTask: Thread {name} ended.')


class DataCalculationTask(Process):

    def __init__(self, calcQueue: multiprocessing.Queue, toAnalyzeQueue: multiprocessing.Queue, calculation_folder: str):
        super().__init__(daemon=True)
        self.calcQueue = calcQueue
        self.toAnalyzeQueue = toAnalyzeQueue
        self.calculation_folder = calculation_folder
        self.logger = logging.getLogger()

    def run(self) -> None:
        while True:
            
            try:
                (city, path) = self.calcQueue.get(timeout=QUEUE_TIMEOUT)
            except Empty:
                self.logger.debug(f'DataCalculationTask: calcQueue is empty!')
                break
    
            self.logger.debug(f'Started to calc {path}')

            output_path = f'{self.calculation_folder}/{city}.json'
            os.system(
                f'python3 external/analyzer.py -i {path} -o {output_path}'
            )

            self.toAnalyzeQueue.put((city, output_path))

            self.logger.debug(f'Result written in {output_path}')


#Переписал для запуска через Pool
class DataAggregationTask():

    @staticmethod
    def run(path: str, rank: int,  wlock: threading.Lock, result_path: str) -> None:
        logger = logging.getLogger()
        thread = threading.current_thread()
        name = thread.name
        
        logger.debug(f'Thread {name}: DataAggregationTask started!')

        readerObj = []
        with open(path, newline='') as f:
            reader = csv.reader(f)
            _ = next(reader)
            readerObj = list(reader)

        for line in readerObj:
            line.append(rank)

        with wlock:
            with open(result_path, newline='', mode='a') as csvfile:
                wr = csv.writer(csvfile, delimiter=',')
                for line in readerObj:
                    wr.writerow(line)

#Переписал для запуска через Pool
class DataAnalyzingTask():

    @staticmethod
    def __transform_df(df: pd.DataFrame, city: str) -> pd.DataFrame:

        df['date'] = pd.to_datetime(df.date)
        df['date'] = df.date.dt.strftime('%d-%m')
        df['день'] = df['date']

        df['Температура, среднее'] = df['temp_avg']
        df['Без осадков, часов'] = df['relevant_cond_hours']

        df = df.drop(columns=['date', 'hours_start', 'hours_end',
                    'hours_count', 'temp_avg', 'relevant_cond_hours'])

        df = df.set_index('день')
        df = df.transpose().rename_axis(' ').reset_index()
        df['Город/день'] = city

        cols = list(df.columns.values)
        cols = [cols[-1]] + cols[0:-1]
        df = df[cols]

        df['Среднее'] = df.iloc[:, 2: -1].mean(axis=1)

        return df

    @staticmethod
    def run(city: str, path: str, analyzed_folder: str) -> dict:
        try: 
            logger = logging.getLogger()
            logger.debug('DataAnalyzingTask started!')

            with open(path) as file:
                data = json.load(file)
                # Про MADRID: отсутсвие "days" в dict давало warning, поэтому не обращал внимания.
                df = DataAnalyzingTask.__transform_df(pd.DataFrame(data.get("days")), city)

            logger.debug(df.to_csv())

            new_path = f'{analyzed_folder}/{city}.txt'
            with open(new_path, 'w') as file:
                file.write(df.to_csv(index=False))

            result = {
                'city': city,
                'temp_avg': df['Среднее'][0],
                'relevant_cond_hours': df['Среднее'][1],
                'path': new_path
            }

            logger.debug(f'DataAnalyzingTask: Wrote in {new_path}')

            return result
        
        except KeyError as err:
            logger.error(f'Empty file {path}. Cant find: {err}')
            return None