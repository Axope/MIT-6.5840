#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <num> <letter>"
    exit 1
fi

num=$1
letter=$2
error_count=0

for ((i = 1; i <= num; i++)); do
    echo "Running test iteration $i..."
    echo "go test -run 2$letter"
    go test -run 2$letter

    if [ $? -ne 0 ]; then
        echo "Test failed!"
        ((error_count++))
    fi

    sleep 1
done

echo "Total errors: $error_count"

if [ $error_count -ne 0 ]; then
    echo "FAILED!"
else 
    echo "PASS!"
fi
