#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <test count>"
    exit 1
fi

num=$1
error_count=0
total_time=0

for ((i = 1; i <= num; i++)); do
    echo "Running test iteration $i..."
    echo "go test -run 2"
    rm -f Raft-*
    start_time=$(date +%s.%N)

    go test -run 2

    if [ $? -ne 0 ]; then
        echo "Test failed!"
        ((error_count++))
    fi

    end_time=$(date +%s.%N)
    execution_time=$(echo "$end_time - $start_time" | bc)
    total_time=$(echo "$total_time + $execution_time" | bc)
    sleep 1
done

echo "Total errors: $error_count"

if [ $error_count -ne 0 ]; then
    echo "FAILED!"
else 
    echo "PASS!"
    echo "Total time: $total_time seconds"
    average_time=$(echo "scale=2; $total_time / $num" | bc)
    echo "Average time per execution: $average_time seconds"
fi
