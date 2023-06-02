import logging
import csv
import multiprocessing
import threading
import pandas as pd
import operator
from queue import Queue, Empty
from pprint import pformat
from pathlib import Path

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES



RESP_PATH = 'data/responses'
CALC_PATH = 'data/calculated'
ANALYZED_PATH = 'data/analyzed'
RESULT_PATH = 'data/result.csv'
DATA_FETCH_NUM_THREADS = 6
CALC_NUM_PROCESSES = 3
ANALYZE_NUM_PROCESSES = 3

def create_folders():
    Path(RESP_PATH).mkdir(parents=True, exist_ok=True)
    Path(CALC_PATH).mkdir(parents=True, exist_ok=True)
    Path(ANALYZED_PATH).mkdir(parents=True, exist_ok=True)

def forecast_weather(cities: str, path = RESULT_PATH):
    """
    Анализ погодных условий по городам
    """

    create_folders()
   
    logger = logging.getLogger()
    logging.basicConfig(
        handlers=[logging.FileHandler(filename='app.log', mode='w')],
        level=logging.DEBUG,
        format='%(process)d: %(asctime)s: %(levelname)s - %(message)s'
    )

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

    analyzeProcesses = []
    for _ in range(ANALYZE_NUM_PROCESSES):
        process = DataAnalyzingTask(analyzeQueue, aggregationQueue, ANALYZED_PATH)
        analyzeProcesses.append(process)
        process.start()

    for proc in analyzeProcesses:
        proc.join()

    ordered = []
    while True:
        try:
            item = aggregationQueue.get(timeout=0.01)

            todict = item[1]
            todict['city'] = item[0]
            ordered.append(todict)
        except Empty:
            logger.debug(f'aggregationQueue is Empty!')
            break
    
    ordered = sorted(ordered, key=operator.itemgetter('relevant_cond_hours'), reverse=True)
    ordered = sorted(ordered, key=operator.itemgetter('temp_avg'), reverse=True) 

    rank = 1
    ordered[0]['rank'] = rank
    for i in range(1, ordered.__len__()):
        if ordered[i-1]['temp_avg'] != ordered[i]['temp_avg'] or ordered[i-1]['relevant_cond_hours'] != ordered[i]['relevant_cond_hours']:
            rank += 1
        ordered[i]['rank'] = rank

    logger.debug('Ranked list created:')
    logger.debug(pformat(ordered))

    [aggregationQueue.put((item['path'], item['rank'])) for item in ordered]

    result_lock = threading.Lock()
    aggregationThreads = []

    cols = ''
    with open(ordered[0]['path'], 'r') as csvFile:
        reader = csv.reader(csvFile)
        cols = ','.join(next(reader)) + ',Рейтинг\n'
    
    with open(RESULT_PATH, newline='', mode='w') as f:
        f.write(cols)
    
    for _ in range(DATA_FETCH_NUM_THREADS):
        thread = DataAggregationTask(aggregationQueue, result_lock, RESULT_PATH)
        fetchThreads.append(thread)
        thread.start()

    for thread in aggregationThreads:
        thread.join()


if __name__ == "__main__":
    forecast_weather(CITIES)
