#!/usr/bin/env python3
import json
import os
import time
import sys

from datetime import datetime, timezone, timedelta
from random import random

SLEEP_SECS = 10


def generate(now_dt):
    timestamps = [
        (now_dt - timedelta(seconds=v)).isoformat()
        for v in range(SLEEP_SECS)
    ]
    timestamps.reverse()

    values = [random() for _ in range(SLEEP_SECS)]
    return {
        'Timestamps': timestamps,
        'Values': values
    }


def spew():
    path = os.path.dirname(__file__)
    now = datetime.now(timezone.utc)
    filename = os.path.join(path, 'cw-fdw-cpu', f'{now}.json')
    with open(filename, 'w') as outfile:
        data = generate(now)
        print(f'Writing to {filename}:\n{data}\n')
        json.dump(data, outfile)

    time.sleep(SLEEP_SECS)


def main():
    while True:
        try:
            spew()
        except KeyboardInterrupt:
            print('\nExiting')
            sys.exit(0)


if __name__ == '__main__':
    main()
