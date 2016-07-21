SLEEP_PERIOD=$1
BATCH_SIZE=$2

OUT_FILE=/home/odroid/bes_flink/data_donotversion/injector.log

echo "sleep period: ${SLEEP_PERIOD}" > ${OUT_FILE}
echo "batch size: ${BATCH_SIZE}" >> ${OUT_FILE}

java bes_flink.BesInjector --rateStatFile /home/odroid/bes_flink/data_donotversion/input_rate.csv --port 12345 --input /home/odroid/bes_flink/data_donotversion/sm1000.sorted.txt --sleepPeriod ${SLEEP_PERIOD} --batchSize ${BATCH_SIZE} &>>${OUT_FILE} &

