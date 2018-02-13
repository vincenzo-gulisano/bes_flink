OFFSET=$1
SLEEP_PERIOD=$2
BATCH_SIZE=$3

OUT_FILE=/home/vincenzo/bes_flink/data_donotversion/injector${OFFSET}.log

rm ${OUT_FILE}
rm /home/vincenzo/bes_flink/data_donotversion/input_rate${OFFSET}.csv

INJECTOR_PORT=$((12345+$OFFSET))

echo "sleep period: ${SLEEP_PERIOD}" > ${OUT_FILE}
echo "batch size: ${BATCH_SIZE}" >> ${OUT_FILE}

java -cp /home/vincenzo/bes_flink/target/fat.bes_flink-0.0.1-SNAPSHOT.jar bes_flink.BesInjector --rateStatFile /home/vincenzo/bes_flink/data_donotversion/input_rate${OFFSET}.csv --port ${INJECTOR_PORT} --input /home/vincenzo/bes_flink/data_donotversion/sm1000.sorted.${OFFSET}.txt --sleepPeriod ${SLEEP_PERIOD} --batchSize ${BATCH_SIZE} &>>${OUT_FILE} &

