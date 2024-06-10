#!/bin/sh
OPS=1000
echo "STRESS TEST - 3 REPLICAS"
echo "Local operations per second: $OPS"
echo "Compiling test..."
if ! go build ./test_stress.go; then
	echo "Compilation error"
	exit 1
fi

SERVER=localhost:5000
# SERVER=mqtt://test.mosquitto.org:1883
# SERVER=mqtt://mqtt.eclipseprojects.io:1883
# SERVER=mqtt://broker.hivemq.com:1883

echo "Run Replica 0"
./test_stress.exe 0 "$SERVER" "$OPS" > /dev/null &
echo "Run Replica 1"
./test_stress.exe 1 "$SERVER" "$OPS" > /dev/null &
echo "Run Replica 2"
./test_stress.exe 2 "$SERVER" "$OPS" > /dev/null &

wait $(jobs -p)
echo "Finished"
