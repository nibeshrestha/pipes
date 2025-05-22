#!/bin/bash

# Simple script to help automate multiple benchmark runs while
# the 'runs' feature of the fabric scripts remains broken (currently
# broken because it does not clear the RocksDB instance between runs,
# and the implementation does not ensure that block hashes are unique
# across runs). Fixing both is TODO.

# Run config. SET BEFORE USE.
BLOCK_SIZES=() # E.g. (10 100 1000 10000)
DEBUG= # E.g. true
DURATION= # E.g. 300
GIT_BRANCHES=() # E.g. ("chained-moonshot" "commit-moonshot" "simple-moonshot")
NODES= # E.g. 10
RUNS= # E.g. 3

if [ -z "$BLOCK_SIZES" ] \
    || [ -z "$DURATION" ] \
    || [ -z "$GIT_BRANCHES" ] \
    || [ -z "$NODES" ] \
    || [ -z "$RUNS" ]
then
    echo "./run_many.sh: [Error] Config not set. Set parameters in script before running." >&2
    exit 1
fi

trap "exit" INT

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
FABFILE="$SCRIPT_DIR/fabfile.py"
LOGS="$SCRIPT_DIR/logs"
LOGS_BACKUP="$SCRIPT_DIR/moonshot_logs_backup"
SETTINGS="$SCRIPT_DIR/settings.json"

[ -f "$FABFILE" ] || (echo "./run_many.sh: [Error] $FABFILE missing" >&2 && exit 1)
[ -f "$SETTINGS" ] || (echo "./run_many.sh: [Error] $SETTINGS missing" >&2 && exit 1)

function get_setting() {
    cat "$SETTINGS" | jq "$1" | xargs
}

# Deployment config.
current_branch="$(get_setting .repo.branch)"
INSTANCE_TYPE="$(get_setting .instances.type | sed s/'\.'/-/g)"

if [ "$DEBUG" = true ]; then
    DEBUG_OPTION="--debug"
    DEBUG_POSTFIX="/debug"
fi

# Update constants in fabfile.
sed -E -i '' s/"'duration': [0-9]+,"/"'duration': $DURATION,"/g "$FABFILE"
sed -E -i '' s/"'nodes': \[[0-9]+\],"/"'nodes': \[$NODES\],"/g "$FABFILE"

for branch in ${GIT_BRANCHES[@]}
do
    # Update the git repository branch in the benchmarker settings.
    sed -i '' s/"\"branch\": \"$current_branch\""/"\"branch\": \"$branch\""/g "$SETTINGS"
    current_branch="$branch"

    # Create root directories for logs and results.
    BACKUP_ROOT="$LOGS_BACKUP/$INSTANCE_TYPE/$current_branch/${DURATION}s$DEBUG_POSTFIX"
    mkdir -p "$BACKUP_ROOT"
    RESULTS_ROOT="$SCRIPT_DIR/moonshot_results/${INSTANCE_TYPE}$DEBUG_POSTFIX"
    mkdir -p "$RESULTS_ROOT"
    RESULTS="$RESULTS_ROOT/${current_branch}.txt"
    echo "Saving results in $RESULTS"

    for block_size in ${BLOCK_SIZES[@]}
    do
        # Ensure we do not overwrite existing logs.
        if [ -d "$BACKUP_ROOT" ]; then
            LOG_NAME_SEP='_'
            LOG_NUMBER_INDEX=5
            CURRENT_END_LOG_INDEX="$(ls "$BACKUP_ROOT" | grep "${NODES}_nodes_${block_size}_" | cut -d"$LOG_NAME_SEP" -f"$LOG_NUMBER_INDEX" | sort -n | tail -n1)"

            if [ -z "$CURRENT_END_LOG_INDEX" ]; then
                CURRENT_END_LOG_INDEX=0
            fi
        fi

        for i in $(seq 1 1 "$RUNS")
        do
            log_index=$((CURRENT_END_LOG_INDEX + i))
            backup="$BACKUP_ROOT/$NODES"_nodes_"$block_size"_certs_"$log_index"
            echo "$(date): Starting run $i of $RUNS (progress will not print until run finishes)..."
            echo "Saving logs in $backup"

            # Update block size in fabfile.
            sed -E -i '' s/"'max_block_size': .*,"/"'max_block_size': $block_size,"/g "$FABFILE"

            if fab remote \
                $DEBUG_OPTION \
                --consensus-only 2>&1 \
                | tee /dev/tty \
                | grep -i "error\|exception\|traceback"
            then
                echo "Failed to complete remote benchmark"
                fab kill
                exit 2
            fi

            if fab logs \
                $DEBUG_OPTION \
                --consensus-only 2>&1 \
                | tee -a "$RESULTS" \
                | tee /dev/tty \
                | grep -i "error\|exception\|traceback"
            then
                echo "Failed to parse logs"
                exit 3
            fi

            cp -r "$LOGS" "$backup";
        done
    done
done
