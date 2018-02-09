
for SLEEP_PERIOD in 100 50 25 10 5 2 1 0
do
for BATCH_SIZE in 1 2 3 4 5
do
for REPETITION in 0 1 2 3 4
do

EXP_FOLDER=/home/vincenzo/bes_flink/exps/exp_${SLEEP_PERIOD}_${BATCH_SIZE}_${REPETITION}
echo "folder in which to store resulting files is ${EXP_FOLDER}"

mkdir ${EXP_FOLDER}

echo "Connecting to sink machine and starting sink"
ssh 10.0.0.112 '/home/vincenzo/bes_flink/cluster_scripts/sink.sh'
echo "Done..."
sleep 2

echo "Connecting to injector machine and starting injector"
ssh 10.46.0.110 '/home/vincenzo/bes_flink/cluster_scripts/injector.sh '"${SLEEP_PERIOD}"' '"${BATCH_SIZE}"' '
echo "Done..."
sleep 2

echo "deploying query to flink"
/home/odroid/flink/flink-1.4.0/bin/flink run -d -c bes_flink.BesOwnWin -p 1 /home/vincenzo/bes/bes_flink-0.0.1-SNAPSHOT.jar --boundX 1.0 --maxCons 19.0 --sinkIP 10.46.0.112 --sinkPort 12346 --injectorIP 10.46.0.110 --injectorPort 12345 --throughputStatFile /tmp/throughput.csv
echo "Done..."
sleep 2

echo "waiting 5 minutes"
sleep 300
/home/vincenzo/flink/flink-1.0.3/bin/flink cancel bes
echo "Done..."
sleep 2

echo "Connecting to sink machine and turning off everything"
ssh 10.46.0.112 'pkill -9 java'

echo "Connecting to injector  machine and turning off everything"
ssh 10.46.0.110 'pkill -9 java'

echo "Collecting files from sink machine"
scp 10.46.0.112:/home/vincenzo/bes_flink/data_donotversion/output_rate.csv ${EXP_FOLDER}
scp 10.46.0.112:/home/vincenzo/bes_flink/data_donotversion/output_latency.csv ${EXP_FOLDER}
scp 10.46.0.112:/home/vincenzo/bes_flink/data_donotversion/sink.log ${EXP_FOLDER}

echo "Collecting files from injector machine"
scp 10.46.0.110:/home/vincenzo/bes_flink/data_donotversion/input_rate.csv ${EXP_FOLDER}
scp 10.46.0.110:/home/vincenzo/bes_flink/data_donotversion/injector.log ${EXP_FOLDER}

echo "Collecting files from slave machine"
scp 10.46.0.111:/tmp/throughput.csv ${EXP_FOLDER}

done
done
done
