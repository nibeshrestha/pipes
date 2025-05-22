import csv
from matplotlib.lines import Line2D
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np

FIG_HEIGHT = 6
FIG_WIDTH = 10
NETWORK_LABEL = 'Network Size (Nodes)'
PAYLOAD_LABEL = 'Payload Size (Bytes)'
PROTOCOLS = [ 'chained-moonshot', 'commit-moonshot', 'narwhal-hs', 'simple-moonshot' ]
RESULTS_ROOT = '../moonshot_results/m5-large/'
# Indexes in the CSV
TIMEOUT_INDEX = 2
RUNTIME_INDEX = 3
# Indexes in the parsed results
NETWORK_SIZE_INDEX = 0
PAYLOAD_SIZE_INDEX = 1
BLOCKS_COMMITTED_INDEX = 2
MEDIAN_LAST_COMMIT_LATENCY_INDEX = 3
MEAN_LAST_COMMIT_LATENCY_INDEX = 4
MEDIAN_FIRST_COMMIT_LATENCY_INDEX = 5
MEAN_FIRST_COMMIT_LATENCY_INDEX = 6
PAYLOAD_ITEM_SIZE = 180 # Bytes (Size of the default Certificate)

METRICS = {
    'Blocks Committed': 2,
    'Median Latency to Last Commit (ms)': 3,
    'Median Latency to First Commit (ms)': 4,
    'Mean Latency to Last Commit (ms)': 5,
    'Mean Latency to First Commit (ms)': 6
}
# Metric labels to indexes in the parsed results
METRICS_NESTED = {
    'Blocks Committed': 0,
    'Median Latency to Last Commit (ms)': 1,
    'Median Latency to First Commit (ms)': 2,
    'Mean Latency to Last Commit (ms)': 3,
    'Mean Latency to First Commit (ms)': 4
}
# Variable labels to indexes in the parsed results
VARIABLES = {
    NETWORK_LABEL: 0,
    PAYLOAD_LABEL: 1
}

def to_float(s):
    return float(s.replace(',', ''))

def update_averages(counts, sums, prev_col1, prev_col2, all_averages):
    averages = [sum_val / counts for sum_val in sums]
    c1 = to_float(prev_col1)
    c2 = to_float(prev_col2)
    configuration_averages = [c1, c2] + averages
    all_averages.append(configuration_averages)
    return all_averages

# Averages the values observed since the last average for each column from column 2 (0-indexed)
# to n of the given CSV file except those given in `cols_to_ignore`, each time the value in
# either column 0 or 1 changes. Returns a nested map indexed by column 0 then column 1 of the
# averages.
def parse_averages(filepath, cols_to_ignore):
    if not filepath.endswith('.csv'):
        return []

    # Initialize variables to store the current values in columns 0 and 1
    prev_col1 = None
    prev_col2 = None
    counts = 0
    # Average results for each unique benchmark configuration
    all_averages = []

    with open(filepath, 'r') as csv_input:
        reader = csv.reader(csv_input, skipinitialspace=True)
        header = next(reader)  # Read and store the header
        cols_to_average = len(header) - 2

        if cols_to_average - len(cols_to_ignore) < 1:
            return []

        # Initialize variables to store sums for averaging
        sums = [0] * (cols_to_average - len(cols_to_ignore))

        for row in reader:
            col1, col2 = row[0], row[1]

            if col1 != prev_col1 or col2 != prev_col2:
                # Calculate and store the average for the previous group of rows
                if counts > 0:
                    all_averages = update_averages(counts, sums, prev_col1, prev_col2, all_averages) 

                # Reset sums and counts for the new group of rows
                sums = [0] * (cols_to_average - len(cols_to_ignore))
                counts = 0

            # Accumulate values for averaging
            for i in range(2, len(row)):
                if i not in cols_to_ignore:
                    sums[i - 2 - len(cols_to_ignore)] += to_float(row[i])
            counts += 1

            prev_col1, prev_col2 = col1, col2

    # Calculate and store the average for the last group of rows
    if counts > 0:
        all_averages = update_averages(counts, sums, prev_col1, prev_col2, all_averages)

    return all_averages

# Turns a map of arrays into a triply-nested map of arrays. Meant for result aggregation.
def map_by(map_of_arrays, level_1_keys_index, level_2_keys_index, remainder_start_index):
    mapped = {}

    for level_0_key, averages in map_of_arrays.items():
        mapped[level_0_key] = {}

        for configuration in averages:
            level_1_key = configuration[level_1_keys_index]
            level_2_key = configuration[level_2_keys_index]

            if level_1_key not in mapped[level_0_key]:
                mapped[level_0_key][level_1_key] = {}
            
            mapped[level_0_key][level_1_key][level_2_key] = configuration[remainder_start_index:]

    return mapped

def map_by_network_size_then_payload_size(protocol_averages_flat):
    # { protocol: { network_size: { payload_size: [ configuration_results... ] } } }
    return map_by(protocol_averages_flat, NETWORK_SIZE_INDEX, PAYLOAD_SIZE_INDEX, BLOCKS_COMMITTED_INDEX)

def map_by_payload_size_then_network_size(protocol_averages_flat):
    # { protocol: { payload_size: { network_size: [ configuration_results... ] } } }
    return map_by(protocol_averages_flat, PAYLOAD_SIZE_INDEX, NETWORK_SIZE_INDEX, BLOCKS_COMMITTED_INDEX)

def compare_averages(protocol_averages, base_protocol):
    protocol_improvements = {}
    base_averages = protocol_averages[base_protocol]

    for protocol, averages in protocol_averages.items():
        # No need to compare to the base protocol.
        if protocol != base_protocol:
            protocol_improvements[protocol] = []

            for i, configuration_result in enumerate(averages):
                # Prepend the configuration settings. We are assuming that each
                # value of protocol_averages has the results for a given configuration
                # at the same index.
                configuration_improvements = configuration_result[:BLOCKS_COMMITTED_INDEX]

                for j, average in enumerate(configuration_result):
                    if j > PAYLOAD_SIZE_INDEX:
                        base_average = base_averages[i][j]

                        if j == BLOCKS_COMMITTED_INDEX:
                            # Throughput
                            percent_improvement = (average - base_average) / base_average * 100
                        else:
                            # Latency
                            percent_improvement = (base_average - average) / base_average * 100


                        configuration_improvements.append(percent_improvement)

                protocol_improvements[protocol].append(configuration_improvements)

    return protocol_improvements

def get_points(protocol_averages, index):
    return [configuration_results[index] for configuration_results in protocol_averages]

def get_metric_points(protocol_averages, metric):
    return get_points(protocol_averages, METRICS[metric])

def three_d_plot(results_by_protocol, metric):
    first_protocol_averages = list(results_by_protocol.values())[0]
    # Parse the network sizes and payload sizes out of the aggregated data.
    network_size_points = get_points(first_protocol_averages, NETWORK_SIZE_INDEX)
    payload_size_points = np.log10(get_points(first_protocol_averages, PAYLOAD_SIZE_INDEX))  # TODO: Correct tick labels

    # Create a 3D figure
    fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT))
    ax = fig.add_subplot(111, projection='3d')

    if metric == "Blocks Committed":
        ax.set_xlabel(PAYLOAD_LABEL)
        ax.set_ylabel(NETWORK_LABEL)
    else:
        ax.set_xlabel(NETWORK_LABEL)
        ax.set_ylabel(PAYLOAD_LABEL)

    ax.set_zlabel(metric)

    for protocol, results in results_by_protocol.items():
        metric_points = get_metric_points(results, metric)

        if metric == "Blocks Committed":
            ax.plot(payload_size_points, network_size_points, metric_points, label=protocol)
        else:
            ax.plot(network_size_points, payload_size_points, metric_points, label=protocol)

    ax.legend()
    plt.show()

def two_d_plot(results_by_protocol, variable, metric, y_label, series_name_suffix):
    # Variables should be the same across all protocols.
    first_protocol_results = list(results_by_protocol.values())[0]
    metric_index = METRICS_NESTED[metric]
    variable_points = first_protocol_results.keys()

    # Create a 2D figure
    fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT))
    ax = fig.add_subplot(111)
    ax.set_xlabel(variable)
    ax.set_ylabel(y_label)

    protocol_series = {}

    for protocol, results in results_by_protocol.items():
        # Index by protocol so we can give each a different marker when plotting.
        series = {}

        for v in variable_points:
            series_ids = results[v].keys()

            for i in series_ids:
                series_name = str(protocol) + ' ' + str(int(i)) + series_name_suffix

                if series_name not in series:
                    series[series_name] = []
                
                configuration_averages = results[v][i]
                metric_point = configuration_averages[metric_index]
                series[series_name].append(metric_point)

        protocol_series[protocol] = series

    # Each protocol should have the same number of series
    num_series = len(list(protocol_series.values())[0])
    # Use colours to identify equivalent results for equivalent configurations across protocols
    colormap = plt.cm.Dark2
    colors = colormap(np.linspace(0, 1, num_series))
    ax.set_prop_cycle('color', colors)

    # Use line styles and markers to differentiate protocols
    line_styles = [s for s, func in Line2D.lineStyles.items() if 'nothing' not in func]
    markers = [m for m, func in Line2D.markers.items() if func != 'nothing' and m in Line2D.filled_markers]

    if 'Payload Size' in variable:
        # Transform values from number of Certificates to Bytes
        ax.set_xscale('log')

    # Plot each series
    for i, series in enumerate(protocol_series.values()):
        for series_name, series_points in series.items():
            ax.plot(variable_points, series_points, label=series_name, linestyle=line_styles[i], marker=markers[i])

    # Actually draws the legend just above the subplot (not lower-centre), which is what we want.
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=len(protocol_series)) #, fancybox=True, shadow=True)
    plt.tight_layout()
    plt.show()

def filter_dict(d, to_keep):
    return { k: v for k, v in d.items() if k in to_keep }

def adjust_payload_sizes(results):
    adjusted_results = []

    for configuration in results:
        adjusted_configuration = [0] * len(configuration)

        for i, item in enumerate(configuration):
            if i == PAYLOAD_SIZE_INDEX:
                adjusted_configuration[i] = item * PAYLOAD_ITEM_SIZE
            else:
                adjusted_configuration[i] = item
        
        adjusted_results.append(adjusted_configuration)
    
    return adjusted_results

def generate_plots():
    improvement_y_label_suffix = ' Improvement vs. Jolteon (%)'
    network_series_suffix = ' Nodes'
    payload_series_suffix = ' Bytes'
    protocol_averages_flat = {}

    for p in PROTOCOLS:
        results_file = RESULTS_ROOT + p + '.csv'
        averages_flat = parse_averages(results_file, [TIMEOUT_INDEX, RUNTIME_INDEX])
        averages_flat = adjust_payload_sizes(averages_flat)
        protocol_averages_flat[p] = averages_flat

    if len(protocol_averages_flat) < 1:
        exit(0)

    average_results = filter_dict(protocol_averages_flat, ['chained-moonshot', 'commit-moonshot', 'narwhal-hs'])
    protocol_averages_by_network_size = map_by_network_size_then_payload_size(average_results)
    protocol_averages_by_payload_size = map_by_payload_size_then_network_size(average_results)

    improvements_over_jolteon = compare_averages(protocol_averages_flat, 'narwhal-hs')
    improvements_over_jolteon_by_network_size = map_by_network_size_then_payload_size(improvements_over_jolteon)
    improvements_over_jolteon_by_payload_size = map_by_payload_size_then_network_size(improvements_over_jolteon)

    for metric in METRICS.keys():
        three_d_plot(protocol_averages_flat, metric)
        two_d_plot(protocol_averages_by_payload_size, PAYLOAD_LABEL, metric, metric, network_series_suffix)
        two_d_plot(protocol_averages_by_network_size, NETWORK_LABEL, metric, metric, payload_series_suffix)
        two_d_plot(improvements_over_jolteon_by_payload_size, PAYLOAD_LABEL, metric, metric + improvement_y_label_suffix, payload_series_suffix)
        two_d_plot(improvements_over_jolteon_by_network_size, NETWORK_LABEL, metric, metric + improvement_y_label_suffix, network_series_suffix)

generate_plots()
