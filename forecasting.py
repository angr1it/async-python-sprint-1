import logging
import csv
import multiprocessing
import threading
import pandas as pd
import operator
from queue import Queue, Empty
from pprint import pformat
from pathlib import Path
from multiprocessing.pool import ThreadPool
import itertools

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES
from settings import (
    RESULT_PATH,
    RESP_PATH,
    CALC_PATH,
    ANALYZED_PATH,
    DATA_FETCH_NUM_THREADS,
    CALC_NUM_PROCESSES,
    ANALYZE_NUM_PROCESSES,
)



logger = logging.getLogger()
logging.basicConfig(
    handlers=[logging.FileHandler(filename='app.log', mode='w')],
    level=logging.DEBUG,
    format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
)


class NotEnoughData(Exception):
    pass

def create_folders():
    Path(RESP_PATH).mkdir(parents=True, exist_ok=True)
    Path(CALC_PATH).mkdir(parents=True, exist_ok=True)
    Path(ANALYZED_PATH).mkdir(parents=True, exist_ok=True)

def rank_analyzed_data(data: list):
    
    data = list(itertools.filterfalse(lambda item: not item , data))
    if data.__len__() < 1:
            message = 'Not enough data to form a rating!'
            logger.error(message)
            raise NotEnoughData(message)
    
    data = sorted(data, key=operator.itemgetter('relevant_cond_hours'), reverse=True)
    data = sorted(data, key=operator.itemgetter('temp_avg'), reverse=True) 

    rank = 1
    data[0]['rank'] = rank
    for i in range(1, data.__len__()):
        if data[i-1]['temp_avg'] != data[i]['temp_avg'] or data[i-1]['relevant_cond_hours'] != data[i]['relevant_cond_hours']:
            rank += 1
        data[i]['rank'] = rank

    logger.debug('Ranked list created:')
    logger.debug(pformat(data))

    return data

def forecast_weather(cities: dict, result_path = RESULT_PATH):
    """
    Анализ погодных условий по городам
    """

    create_folders()

    fetchQueue = Queue()
    calcQueue = multiprocessing.Queue()
    analyzeQueue = multiprocessing.Queue()
    aggregationQueue = multiprocessing.Queue()

    [fetchQueue.put((city, url)) for city, url in cities.items()]

    fetchThreads = []
    for _ in range(DATA_FETCH_NUM_THREADS):
        thread = DataFetchingTask(calcQueue, fetchQueue, RESP_PATH)
        fetchThreads.append(thread)
        thread.start()

    for thread in fetchThreads:
        thread.join()

    calcProcesses = []
    for _ in range(CALC_NUM_PROCESSES):
        process = DataCalculationTask(calcQueue, analyzeQueue, CALC_PATH)
        calcProcesses.append(process)
        process.start()
    
    for proc in calcProcesses:
        proc.join()

    toAnalyze = []
    while True:
        try:
            (city, path) = analyzeQueue.get(timeout=0.01)
            toAnalyze.append((city, path, ANALYZED_PATH))

        except Empty:
            logger.debug('Main: analyzeQueue empty!')
            break
    
    ordered = []
    with multiprocessing.Pool(ANALYZE_NUM_PROCESSES) as pool:
        result = pool.starmap_async(DataAnalyzingTask.run, toAnalyze)
        ordered = result.get()

        logger.debug('DataAnalyzingTask: Thread ended.')

    ordered = rank_analyzed_data(ordered)

    result_lock = threading.Lock()
    items = [(item['path'], item['rank'], result_lock, result_path) for item in ordered]
   
    with open(ordered[0]['path'], 'r') as csvFile:
        reader = csv.reader(csvFile)
        cols = ','.join(next(reader)) + ',Рейтинг\n'
    
    with open(result_path, newline='', mode='w') as f:
        f.write(cols)
    
    with ThreadPool() as pool:
        for _ in pool.starmap(DataAggregationTask.run, items):
            logger.debug('Thread ended.')

    logger.info(f'Город-победитель: {ordered[0]["city"]}')


if __name__ == "__main__":
    forecast_weather(CITIES)
