import csv
import matplotlib.pyplot as plt
from argparse import ArgumentParser
import numpy as np
import scipy.stats as st


def read_csv_file(filename):
    times = []
    metrics = []
    initial_time = 0.0
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            if len(row) == 2:
                time = float(row[0])
                if initial_time == 0.0:
                    initial_time = time
                times.append((time-initial_time)/60)
                metrics.append(float(row[1]))
    return times, metrics


def plot(timesthr, thr, timeslat, lat, output_file_name):
    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('time [s]')
    ax1.set_ylabel('throughput [tuples/min]', color=color)
    ax1.plot(timesthr, thr, color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('latency [ms]', color=color)  # we already handled the x-label with ax1
    ax2.plot(timeslat, lat, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.savefig(output_file_name + ".png")


def main():
    parser = ArgumentParser()
    parser.add_argument("-ft", "--file-throughput", dest="filenamethroughput", help="The CSV input for throughput", required=True)
    parser.add_argument("-fl", "--file-latency", dest="filenamelatency", help="The CSV input for latency", required=True)
    parser.add_argument("-o", "--output", dest="outputfile", help="The output png file name", required=True)

    args = parser.parse_args()

    timesthr, thr = read_csv_file(args.filenamethroughput)
    timeslat, lat = read_csv_file(args.filenamelatency)
    plot(timesthr, thr, timeslat, lat, args.outputfile)
main()