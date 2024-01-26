#!/bin/bash

error_count=0

for i in {1..10}; do
    echo "Running test iteration $i..."
    go test -run 2B

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
