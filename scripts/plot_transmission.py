#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt
from pathlib import Path

TRANSMISSION_FILE_SUFFIX =  '.transmission'

SOR_SEND = 0
SOR_RECV = 1


def parse_time(time_str):
    """
    Parse the given time string:
    Format: "transmission_start_time, transmission_duration"
    """
    parts = time_str.split(',')
    return (int(parts[0]), int(parts[1]))


def collect_data(data, filename):
    """
    Parse the given file and update the map containing the data.
    """
    with open(filename, "r") as file:
        for line in file:
            parts = line.split(';')
            sor = int(parts[0])
            type_name = parts[1]
            channel = int(parts[2])
            source = int(parts[3])
            dest = int(parts[4])
            times = list(map(parse_time, parts[5:]))

            if len(times) == 0:
                continue

            # sort on the timestamp
            times = sorted(times, key=lambda t:t[0])

            if not channel in data:
                data[channel] = dict()
            if not source in data[channel]:
                data[channel][source] = dict()
            if not dest in data[channel][source]:
                data[channel][source][dest] = dict()
            if not type_name in data[channel][source][dest]:
                data[channel][source][dest][type_name] = dict()
            assert not sor in data[channel][source][dest][type_name]
            data[channel][source][dest][type_name][sor] = times


def parse_data_files(data_dir):
    """
    Parser all the transmission files in the given directory.

    Return:
    Transmission data for every channels.
    """
    data = dict()
    path = Path(data_dir)

    for file in path.iterdir():
        if file.suffix == TRANSMISSION_FILE_SUFFIX:
            collect_data(data, file)

    return data


def plot_one(data, title, output_file):
    """
    Plot the figure for the given data (transmission data for one channel and
    one rank, send delay in bleu, and receive in yellow).
    """
    fig, ax = plt.subplots(len(data.keys()), squeeze=False)

    # adjust the padding
    if len(data.keys()) > 0:
        fig.tight_layout(pad=3.0)

    for idx, type_name in enumerate(data):
        # send delays
        send_xmin = [p[0] for p in data[type_name][SOR_SEND]]
        send_xmax = [p[0] + p[1] for p in data[type_name][SOR_SEND]]
        ax[idx, 0].hlines(y=range(len(send_xmin)), xmin=send_xmin, xmax=send_xmax, linewidth=1, color="b")
        # recv delays
        recv_xmin = [p[0] for p in data[type_name][SOR_RECV]]
        recv_xmax = [p[0] + p[1] for p in data[type_name][SOR_RECV]]
        ax[idx, 0].hlines(y=range(len(recv_xmin)), xmin=recv_xmin, xmax=recv_xmax, linewidth=1, color="y")
        # legend
        ax[idx, 0].set_title(type_name)
        ax[idx, 0].set_ylabel("package number")

    ax[len(data.keys()) - 1, 0].set_xlabel("time (ns)")
    fig.suptitle(title)
    plt.savefig(output_file)
    plt.close()


def plot_all(data, output_path):
    """
    Plot all the transmission data for every channel/destination and put the
    resulting figures in the given directory.
    """
    for channel in data:
        for source in data[channel]:
            for dest in data[channel][source]:
                plot_one(data[channel][source][dest],
                         f"transmissions: channel = {channel}, source = {source}, dest = {dest}",
                         output_path / f"channel_{channel}_source_{source}_dest_{dest}.svg")


def main():
    if len(sys.argv) == 3:
        data = parse_data_files(sys.argv[1])
        output_dir = Path(sys.argv[2])

        try:
            output_dir.mkdir()
        except:
            print("Cannot create output dir (make sure the file does not already exists).")
            return
        plot_all(data, output_dir)
    else:
        print("Usage: prog <data_dir> <output_dir>")


if __name__ == "__main__":
    main()
