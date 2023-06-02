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

    def __init__(self, responses: multiprocessing.Queue, cities: Queue, response_folder):
        threading.Thread.__init__(self)
        self.responses = responses
        self.cities = cities
        self.response_folder = response_folder
        self.logger = logging.getLogger()

    def run(self):
        thread = threading.current_thread()
        name = thread.name
        while True:
            try:
                # timeout used in order to raise queue.Empty exception
                (city, url) = self.cities.get(timeout=QUEUE_TIMEOUT)

                self.logger.debug(f'Thread {name} started fetching {city}')
                path = f'{self.response_folder}/{city}.json'

                with open(path, 'w') as file:
                    file.write(json.dumps(
                        YandexWeatherAPI.get_forecasting(url)
                    ))

                self.responses.put((city, path))
                self.logger.debug(f'Thread {name} wrote in {path}')
                self.cities.task_done()

            except Empty:
                self.logger.debug(f'Thread {name} Queue empty!')
                break

            except Exception as ex:
                if ignoreExceptions(ex):
                    self.logger.info(f'Ignored exception: {ex}')
                    continue
                raise Exception(ex)

        self.logger.debug(f'Thread {name} ended.')


class DataCalculationTask(Process):

    def __init__(self, calcQueue: multiprocessing.Queue, toAnalyzeQueue: multiprocessing.Queue, calculation_folder: str):
        super().__init__(daemon=True)
        self.calcQueue = calcQueue
        self.toAnalyzeQueue = toAnalyzeQueue
        self.calculation_folder = calculation_folder
        self.logger = logging.getLogger()

    def run(self):
        while True:
            try:
                if (self.calcQueue.empty()):
                    self.logger.debug('calcQueue is empty!')
                    return

                (city, path) = self.calcQueue.get(timeout=QUEUE_TIMEOUT)

                if (path == None):
                    self.logger.debug('from calcQueue item is None!')
                    return

                self.logger.debug(f'Started to calc {path}')

                output_path = f'{self.calculation_folder}/{city}.json'
                os.system(
                    f'python3 external/analyzer.py -i {path} -o {output_path}'
                )

                self.toAnalyzeQueue.put((city, output_path))

                self.logger.debug(f'Result written in {output_path}')

            except Exception as ex:
                raise Exception(ex)


class DataAggregationTask(threading.Thread):
    def __init__(self, analyzedQueue: multiprocessing.Queue, wlock: threading.Lock, result_path):
        threading.Thread.__init__(self)
        self.analyzedQueue = analyzedQueue
        self.logger = logging.getLogger()
        self.wlock = wlock
        self.result_path = result_path

    def run(self):
        thread = threading.current_thread()
        name = thread.name

        while True:
            try:

                (path, rank) = self.analyzedQueue.get(timeout=QUEUE_TIMEOUT)

                readerObj = []
                with open(path, newline='') as f:
                    reader = csv.reader(f)
                    _ = next(reader)
                    readerObj = list(reader)

                for line in readerObj:
                    line.append(rank)

                with self.wlock:
                    with open(self.result_path, newline='', mode='a') as csvfile:
                        wr = csv.writer(csvfile, delimiter=',')
                        for line in readerObj:
                            wr.writerow(line)

            except Empty:
                self.logger.debug(f'Thread {name} analyzedQueue empty!')
                break


class DataAnalyzingTask(Process):

    def __init__(self, analyzeQueue: multiprocessing.Queue, aggregationQueue: multiprocessing.Queue, analyzed_folder):
        super().__init__()
        self.analyzeQueue = analyzeQueue
        self.aggregationQueue = aggregationQueue
        self.analyzed_folder = analyzed_folder
        self.logger = logging.getLogger()

    def run(self):
        self.logger.debug('DataAnalyzingTask started!')
        while True:
            try:
                item = self.analyzeQueue.get(timeout=QUEUE_TIMEOUT)
                (city, path) = item

                data = json.load(open(path))
                df = pd.DataFrame(data["days"])

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

                self.logger.debug(df.to_csv())
                new_path = f'{self.analyzed_folder}/{city}.txt'
                with open(new_path, 'w') as file:
                    file.write(df.to_csv(index=False))

                value = {
                    'temp_avg': df['Среднее'][0],
                    'relevant_cond_hours': df['Среднее'][1],
                    'path': new_path
                }
                self.aggregationQueue.put((city, value))

                self.logger.debug(f'Wrote in {new_path}')

            except Empty:
                self.logger.debug(f'In DataAnalyzingTask: analyzeQueue empty!')
                break
            
            except Exception as ex:
                self.logger.error(ex)
                raise ex
