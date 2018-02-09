
OUT_FILE=/home/vincenzo/bes_flink/data_donotversion/sink.log

java bes_flink.BesReceiver --port 12346 --rateStatFile /home/odroid/bes_flink/data_donotversion/output_rate.csv --latencyStatFile /home/odroid/bes_flink/data_donotversion/output_latency.csv &>${OUT_FILE} &
