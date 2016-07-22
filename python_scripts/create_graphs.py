from utils import read_file_adjust_timestamps_get_average_within_steady_state, \
    read_file_adjust_timestamps_and_create_graph_time_value_simple, create_graph_multiple_time_value
from statistics import mean

base_folder = '/Users/vinmas/repositories/bes_flink/experiments/160721a/'

input_rates = dict()
throughputs = dict()

keys = []

for batch_size in [1, 2, 3, 4, 5]:

    input_rates['batch' + str(batch_size)] = []
    throughputs['batch' + str(batch_size)] = []
    keys.append('batch' + str(batch_size))

    for sleep_period in [100, 50, 25, 10, 5, 2, 1, 0]:

        input_rate_avgs = []
        throughput_avgs = []

        for repetition in [0, 1, 2, 3, 4]:
            files_folder = 'exp_' + str(sleep_period) + '_' + str(batch_size) + '_' + str(repetition) + '/'

            # IF YOU WANT TO CREATE THE GRAPHS
            [input_rate_ts, input_rate_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
                    base_folder + files_folder + 'input_rate.csv', 'Input rate', 'Time (s)', 'Input rate (t/s)',
                    base_folder + files_folder + 'input_rate.pdf',
            )
            [throughput_ts, throughput_v] = read_file_adjust_timestamps_and_create_graph_time_value_simple(
                    base_folder + files_folder + 'throughput.csv', 'Throughput', 'Time (s)', 'Throughput (t/s)',
                    base_folder + files_folder + 'throughput.pdf',
            )

            # IF YOU JUST WANT THE DATA
            input_rate_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                    base_folder + files_folder + 'input_rate.csv', 60, 240))
            throughput_avgs.append(read_file_adjust_timestamps_get_average_within_steady_state(
                    base_folder + files_folder + 'throughput.csv', 60, 240))

            # # This is for output rate and latency, not used yet
            # read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_rate.csv', 'Output rate', 'Time (s)', 'Output rate (t/s)',
            #         base_folder + files_folder + 'output_rate.pdf',
            # )
            # read_file_adjust_timestamps_and_create_graph_time_value_simple(
            #         base_folder + files_folder + 'output_latency.csv', 'Output latency', 'Time (s)', 'Latency (ms)',
            #         base_folder + files_folder + 'output_latency.pdf',
            # )

        input_rates['batch' + str(batch_size)].append(mean(input_rate_avgs))
        throughputs['batch' + str(batch_size)].append(mean(throughput_avgs))

create_graph_multiple_time_value(input_rates, throughputs, keys, 'Scalability', 'Input rate (t/s)', 'Throughput(t/s)',
                                 base_folder + 'scalability.pdf')
