RESULTS_ROOT="moonshot_results"

if [ ! -d "$RESULTS_ROOT" ]; then
    echo "Error: Results root directory not found" >&2
    exit 1
fi 

for instance_type_root in $(ls "$RESULTS_ROOT")
do
    for results_file in $(ls "$RESULTS_ROOT/$instance_type_root/"*.txt)
    do
        output_csv="${results_file/.txt/.csv}"
        blocks_committed=($(cat "$results_file" | grep "Total Blocks Committed" | sed 's/.* \([0-9]*\)/\1/'))
        block_sizes=($(cat "$results_file" | grep "Block size" | sed 's/Block size: \(.*\) Certificates/\1/'))
        num_nodes=($(cat "$results_file" | grep "Committee size" | sed 's/Committee size: \(.*\) node(s)/\1/'))
        mean_latencies=($(cat "$results_file" | grep "Mean Latency" | sed 's/Mean Latency: \(.*\) ms/\1/'))
        median_latencies=($(cat "$results_file" | grep "Median Latency" | sed 's/Median Latency: \(.*\) ms/\1/'))
        runtimes=($(cat "$results_file" | grep "Execution time" | sed 's/Execution time: \(.*\) s/\1/'))
        timeout_delays=($(cat "$results_file" | grep "Timeout delay" | sed 's/Timeout delay: \(.*\) ms/\1/'))

        echo "\"Network Size (nodes)\", \"Block Size (Certificates)\", \"Timeout Delay (ms)\", \"Runtime (s)\", \"Blocks Committed\", \"Median Latency to Last Commit (ms)\", \"Median Latency to First Commit (ms)\", \"Mean Latency to Last Commit (ms)\", \"Mean Latency to First Commit (ms)\"" > "$output_csv"

        for i in "${!blocks_committed[@]}"
        do
            network_size="${num_nodes[$i]}"
            payload_size="${block_sizes[$i]}"
            runtime="${runtimes[$i]}"
            timeout_delay="${timeout_delays[$i]}"
            throughput_result="${blocks_committed[$i]}"
            first_commit_index=$((i*2))
            last_commit_index=$((i*2+1))
            mean_latency_to_first_commit="${mean_latencies[$first_commit_index]}"
            mean_latency_to_last_commit="${mean_latencies[$last_commit_index]}"
            median_latency_to_first_commit="${median_latencies[$first_commit_index]}"
            median_latency_to_last_commit="${median_latencies[$last_commit_index]}"
            echo "\"$network_size\", \"$payload_size\", \"$timeout_delay\", \"$runtime\", \"$throughput_result\", \"$median_latency_to_last_commit\", \"$median_latency_to_first_commit\", \"$mean_latency_to_last_commit\", \"$mean_latency_to_first_commit\"" >> "$output_csv"
        done
    done
done
