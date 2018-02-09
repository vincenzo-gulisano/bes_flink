
OUT_FILE=/home/vincenzo/bes_flink/data_donotversion/sink.log

java -cp /home/vincenzo/bes_flink/target/fat.bes_flink-0.0.1-SNAPSHOT.jar bes_flink.BesReceiver --port 12346 --rateStatFile /home/vincenzo/bes_flink/data_donotversion/output_rate.csv --latencyStatFile /home/vincenzo/bes_flink/data_donotversion/output_latency.csv &>${OUT_FILE} &
