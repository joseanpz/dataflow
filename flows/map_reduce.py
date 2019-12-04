import prefect
from prefect import task, Flow
from prefect.utilities.tasks import unmapped


@task
def add_one(x):
    """Operates one number at a time"""
    return x + 1


@task
def add(x, y):
    """Operates one number at a time"""
    return x + y


@task(name="sum")
def sum_numbers(y):
    """Operates on an iterable of numbers"""
    result = sum(y)
    logger = prefect.context["logger"]
    logger.info("The sum is {}".format(result))
    return result


@task
def return_list():
    return [1, 2, 3, 4]


@task
def print_hello():
    print("=" * 30)
    print("HELLO")
    print("=" * 30)


# Imperative API
mapred_flow = Flow("Map Reduce")
mapred_flow.set_dependencies(add_one, keyword_tasks={"x": [1, 2, 3, 4]}, mapped=True)
mapred_flow.set_dependencies(sum_numbers, keyword_tasks={"y": add_one})


# Unmapped example
with Flow("unmapped example") as umap_flow:
    result = add.map(x=[1, 2, 3], y=unmapped(5))


# This pattern (combined with the unmapped container) is sometimes useful
# when you have multiple mapping layers and complicated dependency structures.
with Flow("example") as spawn_flow:
    result2 = print_hello.map(upstream_tasks=[return_list])


# Functional API
# with Flow("Map Reduce") as flow:
#     mapped_result = add_one.map(x=[1, 2, 3, 4])
#     summed_result = sum_numbers(mapped_result)
