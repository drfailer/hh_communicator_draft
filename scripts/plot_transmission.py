#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt


def collect_data(filename):
    data = dict()

    with open(filename, "r") as file:
        for line in file:
            parts = line.split(';')
            type_name = parts[0]
            dest = parts[1]
            times = list(map(lambda s: int(s), parts[2:]))
            if not type_name in data:
                data[type_name] = dict()
            data[type_name][dest] = times


def plot(data):
    fig, ax = plt.subplots(len(data.keys()), squeeze=False)

    for idx, type_name in enumerate(data):
        ax[idx, 0].set_title(type_name)
        for dest in data[type_name]:
            ax[idx, 0].plot(data[type_name][dest], label=f"{dest}")

    plt.legend()
    plt.show()


def main():
    data = collect_data(sys.argv[1])
    plot(data)


if __name__ == "__main__":
    main()
