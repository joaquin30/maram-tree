#!/bin/sh
echo "CONSISTENCY TEST - 3 REPLICAS"
echo "Compiling test..."
if ! go build ./test_consistency.go; then
	echo "Compilation error"
	exit 1
fi

rm -f log0.txt log1.txt log2.txt

SERVER=localhost:5000
# SERVER=mqtt://test.mosquitto.org:1883
# SERVER=mqtt://mqtt.eclipseprojects.io:1883
# SERVER=mqtt://broker.hivemq.com:1883

echo "Run Replica 0"
./test_consistency.exe 0 "$SERVER" > log0.txt &
echo "Run Replica 1"
./test_consistency.exe 1 "$SERVER" > log1.txt &
echo "Run Replica 2"
./test_consistency.exe 2 "$SERVER" > log2.txt &

echo "Waiting for CRDTs eventual consistency..."
wait $(jobs -p)

if ! diff log0.txt log1.txt > /dev/null; then
	echo "ERROR: Inconsistent CRDTs 0 and 1"
	exit 1
fi

if ! diff log1.txt log2.txt > /dev/null; then
	echo "ERROR: Inconsistent CRDT 1 and 2"
	exit 1
fi

if ! diff log0.txt log2.txt > /dev/null; then
	echo "ERROR: Inconsistent CRDT 0 and 2"
	exit 1
fi

echo "OK: Test passed"
