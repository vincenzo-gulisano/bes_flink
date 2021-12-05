from utils import read_file_adjust_timestamps_get_average_within_steady_state, \
    read_file_adjust_timestamps_and_create_graph_time_value_simple, create_graph_multiple_time_value, \
    read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value, \
    create_graph_multiple_time_value_paper_version, create_graph_multiple_time_value_log_paper_version
from statistics import mean

### 160724a ###

base_folder = '/Users/vinmas/repositories/bes_flink/data_donotversion/expsRev1/multiple/180214/'

parallelism_x = dict()
input_rates = dict()
throughputs = dict()
latencies = dict()
sorting_order = dict()

keys = []

for main_class in ['BesOwnWin', 'StdAggOwnWin']:

    sleep_period = 0

    key = main_class
    parallelism_x[key] = []
    input_rates[key] = []
    throughputs[key] = []
    latencies[key] = []
    sorting_order[key] = []
    keys.append(key)

    for batch_size in [1]:

        input_rate_avgs = []
        throughput_avgs = []
        latency_avgs = []

        for parallelism in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:

            input_rate_rep_avg = []
            throughput_rep_avg = []
            latency_rep_avg = []

            for repetition in [0, 1, 2]:

                files_folder = str(parallelism) + '/' + main_class + '/exp_' + str(sleep_period) + '_' + str(
                    batch_size) + '_' + str(repetition) + '/'

                input_rate = []
                throughput = []
                latency = []

                for instance in range(1, parallelism + 1):
                    # IF YOU JUST WANT THE DATA
                    input_rate_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                        base_folder + files_folder + 'input_rate' + str(instance) + '.csv', 60, 240))
                    throughput_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                        base_folder + files_folder + 'throughput' + str(instance) + '.csv', 60, 240))
                    latency_avgs.append(
                        read_file_adjust_timestamps_get_average_within_steady_state_ignore_special_value(
                            base_folder + files_folder + 'output_latency' + str(instance) + '.csv', 60, 240, -1))

                    input_rate.append(mean(input_rate_avgs))
                    throughput.append(mean(throughput_avgs))
                    latency.append(mean(latency_avgs))

                input_rate_rep_avg.append(sum(input_rate))
                throughput_rep_avg.append(sum(throughput))
                latency_rep_avg.append(mean(latency))

            parallelism_x[key].append(parallelism)
            input_rates[key].append(mean(input_rate_rep_avg))
            throughputs[key].append(mean(throughput_rep_avg))
            latencies[key].append(mean(latency_rep_avg))
            print(str(parallelism) + '\t' + main_class + '\t' + str(mean(input_rate_rep_avg)) + '\t' + str(
                mean(throughput_rep_avg)) + '\t' + str(mean(latency_rep_avg)))

        sorting_order[key] = [i[0] for i in sorted(enumerate(input_rates[key]), key=lambda x: x[1])]

create_graph_multiple_time_value(parallelism_x, throughputs, keys,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Parallelism', 'Throughput (e/s)',
                                               base_folder + 'scalabilitythroughput.pdf')
create_graph_multiple_time_value(parallelism_x, latencies, keys,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Parallelism', 'Latency (ms)',
                                               base_folder + 'scalabilitylatency.pdf')

create_graph_multiple_time_value_paper_version(parallelism_x, throughputs, keys, sorting_order,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Parallelism', 'Throughput (e/s)',
                                               '/Users/vinmas/repositories/dpds_shared/BESFGCS2018CR/fig/scalabilitythroughput.pdf')
create_graph_multiple_time_value_paper_version(parallelism_x, latencies, keys, sorting_order,
                                               {'BesOwnWin': 'Bes', 'StdAggOwnWin': 'PO'},
                                               'Parallelism', 'Latency (ms)',
                                               '/Users/vinmas/repositories/dpds_shared/BESFGCS2018CR/fig/scalabilitylatency.pdf')
