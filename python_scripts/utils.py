from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
from statistics import mean
import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter


def create_graph_time_value_simple(x, y, title, x_label, y_label, out_file):
    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(out_file)

    f = plt.figure()

    plt.plot(x, y)

    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_graph_multiple_time_value(xs, ys, keys, title, x_label, y_label, outFile):
    markers = ['.', ',', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd',
               '|', '_']

    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    ax = plt.gca()

    marker_i = 0
    for key in keys:
        plt.plot(xs[key], ys[key], label=key, marker=markers[marker_i])
        marker_i = (marker_i + 1) % len(markers)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True)
    plt.legend(loc='best')

    ymin, ymax = plt.ylim()
    plt.ylim(0, ymax)

    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_graph_multiple_time_value_paper_version(xs, ys, keys, sorting_orders, legend_entries, x_label, y_label,
                                                   outFile):
    markers = ['.', ',', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd',
               '|', '_']

    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    f.set_size_inches(6.88, 5.4)
    ax = plt.gca()

    marker_i = 0
    for key in keys:
        plt.plot(itemgetter(*sorting_orders[key])(xs[key]), itemgetter(*sorting_orders[key])(ys[key]),
                 label=legend_entries[key])  # , marker=markers[marker_i]
        marker_i = (marker_i + 1) % len(markers)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.grid(True)
    plt.legend(loc='best')

    ymin, ymax = plt.ylim()
    plt.ylim(0, ymax)

    plt.close()

    pp.savefig(f)
    pp.close()

    return


def create_graph_multiple_time_value_log_paper_version(xs, ys, keys, sorting_orders, legend_entries, x_label, y_label,
                                                       outFile):
    markers = ['.', ',', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd',
               '|', '_']

    rcParams.update({'figure.autolayout': True})
    pp = PdfPages(outFile)

    f = plt.figure()
    f.set_size_inches(6.88, 5.4)
    ax = plt.gca()

    marker_i = 0
    for key in keys:
        plt.plot(itemgetter(*sorting_orders[key])(xs[key]), itemgetter(*sorting_orders[key])(ys[key]),
                 label=legend_entries[key])  # , marker=markers[marker_i]
        marker_i = (marker_i + 1) % len(markers)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    ax.set_yscale('log')
    plt.grid(True)
    plt.legend(loc='best')

    ymin, ymax = plt.ylim()
    # plt.ylim(0, ymax)

    plt.close()

    pp.savefig(f)
    pp.close()

    return


def read_file_simple(input_file):
    t, v = np.genfromtxt(input_file, dtype=int, delimiter=',').transpose()
    return t, v


def read_file_and_adjust_timestamps(input_file, division_factor=1000):
    [t, v] = read_file_simple(input_file)
    t -= t[0]
    t /= 1000
    return t, v


def read_file_adjust_timestamps_and_create_graph_time_value_simple(input_file, title, x_label, y_label, out_file,
                                                                   division_factor=1000):
    [t, v] = read_file_and_adjust_timestamps(input_file, division_factor)
    create_graph_time_value_simple(t, v, title, x_label, y_label, out_file)
    return t, v


def read_file_adjust_timestamps_get_average_within_steady_state(input_file, steady_state_start, steady_state_end,
                                                                division_factor=1000):
    [t, v] = read_file_and_adjust_timestamps(input_file, division_factor)
    return mean(v[steady_state_start:steady_state_end])


def read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value(input_file, steady_state_start,
                                                                                     steady_state_end,
                                                                                     special_value,
                                                                                     division_factor=1000):
    [t, v] = read_file_and_adjust_timestamps(input_file, division_factor)
    indices = [i for i, x in enumerate(v) if i >= steady_state_start and i <= steady_state_end and x != special_value]
    return mean(v[indices])

# def create_graph_time_value(x, y, title, x_label, y_label, outFile, step=0):
#     rcParams.update({'figure.autolayout': True})
#     pp = PdfPages(outFile)
#
#     f = plt.figure()
#     ax = plt.gca()
#
#     plt.plot(x, y)
#
#     if step > 0:
#         counter = 1
#         for i in range(int(x[0]) + step, int(x[-1]), step):
#             plt.plot([i, i], [min(y), max(y)], color='r')
#             indexes = [index for index, value in enumerate(x) if value >= i - step and value < i]
#             plt.text(i, min(y), str(counter) + '\n' + str(int(statistics.median(y[indexes[0]:indexes[-1]]))),
#                      verticalalignment='bottom', horizontalalignment='right', fontsize=7)
#             counter += 1
#
#     plt.xlabel(x_label)
#     plt.ylabel(y_label)
#     plt.title(title)
#     plt.grid(True)
#     plt.close()
#
#     pp.savefig(f)
#     pp.close()
#
#     return
