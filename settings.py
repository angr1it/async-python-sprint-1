import os

RESP_PATH = 'data/responses'
CALC_PATH = 'data/calculated'
ANALYZED_PATH = 'data/analyzed'
RESULT_PATH = 'data/result.csv'
DATA_FETCH_NUM_THREADS = 6
CALC_NUM_PROCESSES = max(1, os.cpu_count() - 1)
ANALYZE_NUM_PROCESSES = max(1, os.cpu_count() - 1)