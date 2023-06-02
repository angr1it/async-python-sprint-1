import unittest
import multiprocessing
from queue import Queue
import json
import threading
import re


from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES
import forecasting


TEST_RESP_PATH = 'tests/data/responses'
TEST_CALC_PATH = 'tests/data/calculated'
TEST_ANALYZED_PATH = 'tests/data/analyzed'
TEST_RESULT_PATH = 'tests/data/result.csv'
TEST_FORECASTING_RESULT_PATH = 'tests/data/result_forecast.csv'


class TestDataFetchingTask(unittest.TestCase):

    def test_DataFetchingTask(self):
        fetchQueue = Queue()
        calcQueue = multiprocessing.Queue()
        fetchQueue.put(('MOSCOW', CITIES['MOSCOW']))

        thread = DataFetchingTask(calcQueue, fetchQueue, TEST_RESP_PATH)
        thread.start()
        thread.join()

        item = calcQueue.get()
        self.assertEqual(item, ('MOSCOW', 'tests/data/responses/MOSCOW.json'))

        (_, path) = item
        file = open(path)
        data = json.load(file)
        self.assertIn('forecasts', data)


class TestDataCalculationTask(unittest.TestCase):   

    def test_DataCalculationTask(self):
        calcQueue = multiprocessing.Queue()
        analyzeQueue = multiprocessing.Queue()
        calcQueue.put(('BEIJING', 'tests/data/responses/BEIJING.json'))

        process = DataCalculationTask(calcQueue, analyzeQueue, TEST_CALC_PATH)
        process.start()
        process.join()

        item = analyzeQueue.get()
        self.assertEqual(item, ('BEIJING', 'tests/data/calculated/BEIJING.json'))

        (_, path) = item
        file = open(path)
        data = json.load(file)
        self.assertIn('days', data)

        days = data['days']
        self.assertEqual(5, days.__len__())
        self.assertIn('date', days[0])
        self.assertIn('temp_avg', days[0])
        self.assertIn('relevant_cond_hours', days[0])


class TestDataAnalyzingTask(unittest.TestCase):   

    def test_DataAnalyzingTask(self):

        analyzeQueue = multiprocessing.Queue()
        aggregationQueue = multiprocessing.Queue()
        analyzeQueue.put(('BERLIN', 'tests/data/calculated/BERLIN.json'))

        process = DataAnalyzingTask(analyzeQueue, aggregationQueue, TEST_ANALYZED_PATH)
        process.start()
        process.join()

        (city, obj) = aggregationQueue.get()
        
        self.assertEqual(city, 'BERLIN')
        self.assertEqual(obj['path'], 'tests/data/analyzed/BERLIN.txt')
        self.assertIn('temp_avg', obj)
        self.assertIn('relevant_cond_hours', obj)

        #TODO: write asserts for file content


class TestDataAggregationTask(unittest.TestCase):   

    def test_DataAggregationTask(self):

        aggregationQueue = multiprocessing.Queue()
        aggregationQueue.put(('tests/data/analyzed/BUCHAREST.txt', 1))

        with open(TEST_RESULT_PATH, newline='', mode='w') as f:
            f.write('')
        process = DataAggregationTask(aggregationQueue, threading.Lock(), TEST_RESULT_PATH)
        process.start()
        process.join()

        with open(TEST_RESULT_PATH, newline='', mode='r') as f:
            doc = f.read()
        
        result = '''\
        BUCHAREST,"Температура, среднее",27.455,26.091,27.818,18.0,,24.841,1
        BUCHAREST,"Без осадков, часов",11.0,11.0,11.0,1.0,0.0,8.5,1
        '''

        self.assertEqual(re.sub('[^A-Za-z0-9]+', '', result), re.sub('[^A-Za-z0-9]+', '', doc))


if __name__ == '__main__':
    unittest.main()