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


def compute_jitter(metrics):
    mean = np.mean(metrics)
    jitter = []
    for metric in metrics:
        jitter.append(abs(metric - mean))

    return jitter


def compute_jitter_statistics(jitter, output_file_name):
    mean = np.mean(jitter)
    conf_interval = confidence_interval(jitter, 0.95)
    perc99 = np.percentile(jitter, 99)
    perc999 = np.percentile(jitter, 99.9)

    f = open(output_file_name+"_jitter.blfm", "w")
    print("mean=" + str(mean) + "\nconf_interval=" + str(conf_interval) + "\nperc99" + str(perc99) + "\nperc999" + str(perc999) + "\nn=" + str(len(jitter)), file=f)
    f.close()

def confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    se = st.sem(a)
    h = se * st.t.ppf((1 + confidence) / 2., n-1)
    return h


def plot(times, metrics, yaxis, output_file_name, shrink):
    new_times = []
    new_metrics = []
    for i in range(0, len(times), shrink):
        if i <= len(times):
            new_times.append(times[i])
            new_metrics.append(metrics[i])
    plt.cla()
    plt.clf()
    plt.plot(new_times, new_metrics)
    plt.xlabel("time [min]")
    plt.ylabel(yaxis)
    plt.savefig(output_file_name + ".png")

def main():
    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="filename", help="The CSV input file", required=True)
    parser.add_argument("-o", "--output", dest="outputfile", help="The output png file name", required=True)
    parser.add_argument("-y", "--yaxislabel", dest="yaxislabel", help="The label for the y axis", required=True)
    parser.add_argument("-j", "--jaxis", dest="jaxis", help="The jitter y axis label", required=True)
    parser.add_argument("-s", "--step", dest="step", help="The plot step resolution", default=1)

    args = parser.parse_args()

    times, metrics = read_csv_file(args.filename)
    jitter = compute_jitter(metrics)
    compute_jitter_statistics(jitter, args.outputfile)
    plot(times, jitter, args.jaxis, args.outputfile+"_jitter", int(args.step))
    plot(times, metrics, args.yaxislabel, args.outputfile, int(args.step))
main()