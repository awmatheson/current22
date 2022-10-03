import os
import csv
import pandas as pd
import itertools as it

from river import anomaly
from river import utils
from river import model_selection, metrics, evaluate

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig, distribute
from bytewax.outputs import StdOutputConfig

grid = {
    'n_trees': [x for x in range(3,6)],
    'height': [x for x in range(3,6)],
    'window_size': [x for x in range(50,180,20)]
}
param_values = list(it.product(
    grid['n_trees'],
    grid['height'],
    grid['window_size']))

def process_files(param_values, state):
    for model_params in param_values:
        state = state or 0
        for i, line in enumerate(model_params):
            state += 1
            yield (state, (str(model_params), model_params))

def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    workers_files = distribute(param_values, worker_index, worker_count)
    return process_files(workers_files, state)

def evaluate_model(params__params):
    param_set, params = params__params
    dataset = get_dataset()
    model = anomaly.HalfSpaceTrees(
        n_trees=params[0],
        height=params[1],
        window_size=params[2],
        limits={'x': (0.0, 500.0)},
        seed=11
    )
    auc = metrics.ROCAUC()
    for value in dataset:
        score = model.score_one({"x": value['x']})
        model = model.learn_one({"x": value['x']})
        auc = auc.update(value['Y'], score)
    
    return param_set, auc

def get_dataset():
    data = pd.read_csv('test/(38.750442 -120.935794) test_set.csv')
    data['x'] = data[['PM2.5_CF1_ug/m3']]
    return data[['x','Y']].to_dict(orient='records')

dataset = get_dataset()

flow = Dataflow()
flow.input("model_params", ManualInputConfig(input_builder))
flow.map(evaluate_model)
flow.capture(StdOutputConfig())

if __name__ == "__main__":
    spawn_cluster(flow, **parse.cluster_args())
