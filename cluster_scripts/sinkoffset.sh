OFFSET=$1
OUT_FILE=/home/vincenzo/bes_flink/data_donotversion/sink${OFFSET}.log
SINK_PORT=$((12346+$OFFSET))
java -cp /home/vincenzo/bes_flink/target/fat.bes_flink-0.0.1-SNAPSHOT.jar bes_flink.BesReceiver --port ${SINK_PORT} --rateStatFile /home/vincenzo/bes_flink/data_donotversion/output_rate${OFFSET}.csv --latencyStatFile /home/vincenzo/bes_flink/data_donotversion/output_latency${OFFSET}.csv &>${OUT_FILE} &
