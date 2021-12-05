from utils import read_file_adjust_timestamps_get_average_within_steady_state, \
    read_file_adjust_timestamps_and_create_graph_time_value_simple, create_graph_multiple_time_value, \
    read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value, \
    create_graph_multiple_time_value_paper_version, create_graph_multiple_time_value_log_paper_version
from statistics import mean

### 160724a ###

base_folder = '/Users/vinmas/repositories/bes_flink/data_donotversion/expsRev1/180213/'

input_rates = dict()
throughputs = dict()
latencies = dict()
sorting_order = dict()

keys = []

for main_class in ['BesOwnWin', 'StdAggOwnWin']:

    sleep_period = 10

    key = main_class
    input_rates[key] = []
    throughputs[key] = []
    latencies[key] = []
    sorting_order[key] = []
    keys.append(key)

    for batch_size in [10, 50, 100, 150, 200, 250, 300]:

        input_rate_avgs = []
        throughput_avgs = []
        latency_avgs = []

        for repetition in [0, 1, 2]:

            files_folder = main_class + '/exp_' + str(sleep_period) + '_' + str(batch_size) + '_' + str(repetition) + '/'

            # IF YOU WANT TO CREATE THE GRAPHS
            # [input_rate_ts, input_rate_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'input_rate.csv', 'Input rate', 'Time (s)', 'Input rate (t/s)',
            #         base_folder + files_folder + 'input_rate.pdf',
            # )
            # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'throughput.csv', 'Throughput', 'Time (s)', 'Throughput (t/s)',
            #         base_folder + files_folder + 'throughput.pdf',
            # )
            # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
            #         base_folder + files_folder + 'output_rate.pdf',
            # )
            # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
            #         base_folder + files_folder + 'output_latency.pdf',
            # )

            # IF YOU JUST WANT THE DATA
            input_rate_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                    base_folder + files_folder + 'input_rate.csv', 60, 120))
            throughput_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                    base_folder + files_folder + 'throughput.csv', 60, 120))
            latency_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value(
                    base_folder + files_folder + 'output_latency.csv', 60, 120, -1))

            # # This is for output rate and latency, not used yet
            # read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
            #         base_folder + files_folder + 'output_rate.pdf',
            # )
            # read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
            #         base_folder + files_folder + 'output_latency.pdf',
            # )

        input_rates[key].append(mean(input_rate_avgs))
        throughputs[key].append(mean(throughput_avgs))
        latencies[key].append(mean(latency_avgs))

    sorting_order[key] = [i[0] for i in sorted(enumerate(input_rates[key]), key=lambda x:x[1])]

create_graph_multiple_time_value(input_rates, throughputs, keys, 'Scalability', 'Input rate (t/s)', 'Throughput(t/s)',
                                 base_folder + 'scalability.pdf')
create_graph_multiple_time_value(input_rates, latencies, keys, 'Latency', 'Input rate (t/s)', 'Latency(ms)',
                                 base_folder + 'latency.pdf')
create_graph_multiple_time_value_paper_version(input_rates, throughputs, keys, sorting_order,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Input rate (e/s)', 'Throughput (e/s)',
                                               '/Users/vinmas/repositories/dpds_shared/BESFGCS2018CR/fig/throughput.pdf')
create_graph_multiple_time_value_paper_version(input_rates, latencies, keys, sorting_order,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Input rate (e/s)', 'Latency (ms)',
                                               '/Users/vinmas/repositories/dpds_shared/BESFGCS2018CR/fig/latency.pdf')

### 160723a ###

### Notice: throughput file is throughput.csv0 because conv operator could be parallel ###

# base_folder = '/Users/vinmas/repositories/bes_flink/experiments/160723a/'
#
# input_rates = dict()
# throughputs = dict()
# latencies = dict()
#
# keys = []
#
# for sleep_period in [10]:
#
#     key = 'sleep' + str(sleep_period)
#     input_rates[key] = []
#     throughputs[key] = []
#     latencies[key] = []
#     keys.append(key)
#
#     for batch_size in [5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53, 56]:
#
#         input_rate_avgs = []
#         throughput_avgs = []
#         latency_avgs = []
#
#         for repetition in [0, 1, 2, 3, 4]:
#             files_folder = 'exp_' + str(sleep_period) + '_' + str(batch_size) + '_' + str(repetition) + '/'
#
#             # # IF YOU WANT TO CREATE THE GRAPHS
#             # [input_rate_ts, input_rate_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'input_rate.csv', 'Input rate', 'Time (s)', 'Input rate (t/s)',
#             #         base_folder + files_folder + 'input_rate.pdf',
#             # )
#             # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'throughput.csv0', 'Throughput', 'Time (s)', 'Throughput (t/s)',
#             #         base_folder + files_folder + 'throughput.pdf',
#             # )
#             # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
#             #         base_folder + files_folder + 'output_rate.pdf',
#             # )
#             # [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
#             #         base_folder + files_folder + 'output_latency.pdf',
#             # )
#
#             # IF YOU JUST WANT THE DATA
#             input_rate_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
#                     base_folder + files_folder + 'input_rate.csv', 60, 240))
#             throughput_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
#                     base_folder + files_folder + 'throughput.csv0', 60, 240))
#             latency_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value(
#                     base_folder + files_folder + 'output_latency.csv', 60, 240, -1))
#
#             # # This is for output rate and latency, not used yet
#             # read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
#             #         base_folder + files_folder + 'output_rate.pdf',
#             # )
#             # read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
#             #         base_folder + files_folder + 'output_latency.pdf',
#             # )
#
#         input_rates[key].append(mean(input_rate_avgs))
#         throughputs[key].append(mean(throughput_avgs))
#         latencies[key].append(mean(latency_avgs))
#
# create_graph_multiple_time_value(input_rates, throughputs, keys, 'Scalability', 'Input rate (t/s)', 'Throughput(t/s)',
#                                  base_folder + 'scalability.pdf')
# create_graph_multiple_time_value(input_rates, latencies, keys, 'Latency', 'Input rate (t/s)', 'Latency(ms)',
#                                  base_folder + 'latency.pdf')

### 160721a ###

# base_folder = '/Users/vinmas/repositories/bes_flink/experiments/160721a/'
#
# input_rates = dict()
# throughputs = dict()
#
# keys = []
#
# for batch_size in [1, 2, 3, 4, 5]:
#
#     input_rates['batch' + str(batch_size)] = []
#     throughputs['batch' + str(batch_size)] = []
#     keys.append('batch' + str(batch_size))
#
#     for sleep_period in [100, 50, 25, 10, 5, 2, 1, 0]:
#
#         input_rate_avgs = []
#         throughput_avgs = []
#
#         for repetition in [0, 1, 2, 3, 4]:
#             files_folder = 'exp_' + str(sleep_period) + '_' + str(batch_size) + '_' + str(repetition) + '/'
#
#             # IF YOU WANT TO CREATE THE GRAPHS
#             [input_rate_ts, input_rate_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#                     base_folder + files_folder + 'input_rate.csv', 'Input rate', 'Time (s)', 'Input rate (t/s)',
#                     base_folder + files_folder + 'input_rate.pdf',
#             )
#             [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
#                     base_folder + files_folder + 'throughput.csv', 'Throughput', 'Time (s)', 'Throughput (t/s)',
#                     base_folder + files_folder + 'throughput.pdf',
#             )
#
#             # IF YOU JUST WANT THE DATA
#             input_rate_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
#                     base_folder + files_folder + 'input_rate.csv', 60, 240))
#             throughput_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
#                     base_folder + files_folder + 'throughput.csv', 60, 240))
#
#             # # This is for output rate and latency, not used yet
#             # read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
#             #         base_folder + files_folder + 'output_rate.pdf',
#             # )
#             # read_file_adjust_timestamps_and_create_graph_time_value_simple(
#             #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
#             #         base_folder + files_folder + 'output_latency.pdf',
#             # )
#
#         input_rates['batch' + str(batch_size)].append(mean(input_rate_avgs))
#         throughputs['batch' + str(batch_size)].append(mean(throughput_avgs))
#
# create_graph_multiple_time_value(input_rates, throughputs, keys, 'Scalability', 'Input rate (t/s)', 'Throughput(t/s)',
#                                  base_folder + 'scalability.pdf')
