import datetime

from flows.etl import flow


def main():
    from prefect.schedules import IntervalSchedule

    every_minute = IntervalSchedule(
        start_date=datetime.datetime.utcnow(),
        interval=datetime.timedelta(minutes=1)
    )
    flow.schedule = every_minute
    flow.run()  # runs this flow on its schedule


if __name__ == "__main__":
    state = flow.run()
    print(state)
