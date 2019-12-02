from prefect import task, Flow
import random
from time import sleep, time


a = list(range(10))  # [1, 2, 3, 4, 5]
b = list(range(10))  # [5, 4, 3, 2, 1]


@task
def inc(x):
    # sleep(random.random() / 10)
    return x + 1


@task
def dec(x):
    # sleep(random.random() / 10)
    return x - 1


@task
def add(x, y):
    # sleep(random.random() / 10)
    return x + y


@task(name="sum", )
def list_sum(arr):
    return sum(arr)


with Flow("dask-example") as dask_flow:
    incs = inc.map(a)
    decs = dec.map(b)
    adds = add.map(incs, decs)
    total = list_sum(adds)
