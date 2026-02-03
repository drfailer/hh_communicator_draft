#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt


def parse_time(time_str):
    parts = time_str.split(',')
    return (int(parts[0]), int(parts[1]))


def collect_data(filename):
    data = dict()

    with open(filename, "r") as file:
        for line in file:
            parts = line.split(';')
            type_name = parts[0]
            channel = int(parts[1])
            dest = int(parts[2])
            times = list(map(parse_time, parts[3:]))

            if not channel in data:
                data[channel] = dict()
            if not dest in data[channel]:
                data[channel][dest] = dict()
            data[channel][dest][type_name] = times

    return data


def plot_one(data, channel, dest):
    if not channel in data:
        print("error: the requested channel is not present it the given data.")
        return
    if not dest in data[channel]:
        print("error: the requested destintation is not present it the given data.")
        return
    fig, ax = plt.subplots(len(data[channel][dest].keys()), squeeze=False)

    for idx, type_name in enumerate(data[channel][dest]):
        xmin = [p[0] for p in data[channel][dest][type_name]]
        xmax = [p[0] + p[1] for p in data[channel][dest][type_name]]
        ax[idx, 0].hlines(y=range(len(xmax)), xmin=xmin, xmax=xmax, linewidth=1)
        ax[idx, 0].set_title(type_name)
        ax[idx, 0].set_ylabel("transmissions")

    ax[len(data[channel][dest].keys()) - 1, 0].set_xlabel("time (ns)")
    fig.suptitle(f"transmissions from {channel} to {dest}")
    plt.show()


def main():
    if len(sys.argv) == 4:
        data = collect_data(sys.argv[1])
        plot_one(data, int(sys.argv[2]), int(sys.argv[3]))
    else:
        print("Usage: prog <data_file> <channel> <destination_rank>")


if __name__ == "__main__":
    main()
