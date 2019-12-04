import datetime
from time import time

from flows.dask import dask_flow
from flows.etl import etl_flow
from flows.map_reduce import mapred_flow, umap_flow, spawn_flow
from flows.task_looping import loop_flow, mapped_loop_flow


def main():
    from prefect.schedules import IntervalSchedule

    every_minute = IntervalSchedule(
        start_date=datetime.datetime.utcnow(),
        interval=datetime.timedelta(minutes=1)
    )
    dask_flow.schedule = every_minute
    dask_flow.run()  # runs this flow on its schedule


if __name__ == "__main__":
    flow = loop_flow
    start_time = time()
    state = flow.run(M=100)
    terminal_tasks = flow.terminal_tasks()
    subterminal_tasks = [list(flow.upstream_tasks(terminal_task)) for terminal_task in terminal_tasks]
    # [edge.upstream_task for edge in list(flow.edges_to(terminal_task))]

    initial_tasks = flow.root_tasks()
    secondary_tasks = [list(flow.downstream_tasks(initial_task)) for initial_task in initial_tasks]
    # [edge.downstream_task for edge in list(flow.edges_from(initial_tasks))]

    print(f'Total time: {time()-start_time}')
    print(state)

    print([state.result[initial_task].result for initial_task in initial_tasks])
    print(
        [[state.result[secondary_task].result for secondary_task in secondary_node_task] for secondary_node_task
         in secondary_tasks])

    print('...')

    print(
        [[state.result[subterminal_task].result for subterminal_task in subterminal_node_task] for subterminal_node_task
         in subterminal_tasks])
    print([state.result[terminal_task].result for terminal_task in terminal_tasks])
