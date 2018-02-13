
for SLEEP_PERIOD in 10
do
for BATCH_SIZE in 10 50 100 150 200 250 300
do
for REPETITION in 0 1 2
do
for METHOD in BesOwnWin StdAggOwnWin
do


for i in 2 # First loop.
do

    mkdir /home/vincenzo/bes_flink/exps/$i/
    mkdir /home/vincenzo/bes_flink/exps/$i/${METHOD}/

    EXP_FOLDER=/home/vincenzo/bes_flink/exps/$i/${METHOD}/exp_${SLEEP_PERIOD}_${BATCH_SIZE}_${REPETITION}
    echo "folder in which to store resulting files is ${EXP_FOLDER}"
    mkdir ${EXP_FOLDER}

    for j in $(seq 1 $i)
    do

	echo "Connecting to sink machine and starting sink"
	ssh 10.0.0.112 '/home/vincenzo/bes_flink/cluster_scripts/sinkoffset.sh '"${j}"' '
	echo "Done..."
	sleep 2

    done

    for j in $(seq 1 $i)
    do

	echo "Connecting to injector machine and starting injector"
	ssh 10.0.0.110 '/home/vincenzo/bes_flink/cluster_scripts/injectoroffset.sh '"${j}"' '"${SLEEP_PERIOD}"' '"${BATCH_SIZE}"' '
	echo "Done..."
	sleep 2

    done

    for j in $(seq 1 $i)
    do

	echo "deploying query to flink"
	/home/vincenzo/flink/flink-1.4.0/bin/flink run -d -c bes_flink.${METHOD} -p 1 /home/vincenzo/bes_flink/target/bes_flink-0.0.1-SNAPSHOT.jar --bound 1.0 --maxCons 19.0 --sinkIP 10.0.0.112 --sinkPort 12346 --injectorIP 10.0.0.110 --injectorPort 12345 --throughputStatFile /tmp/throughput.csv --AppName bes${j}
	echo "Done..."

    done

done







echo "waiting 3 minutes"
sleep 180
JOBID="$(/home/vincenzo/flink/flink-1.4.0/bin/flink list | grep "bes (RUNNING)" | cut -d' ' -f 4)"
/home/vincenzo/flink/flink-1.4.0/bin/flink cancel $JOBID
echo "Done..."
sleep 2

echo "Connecting to sink machine and turning off everything"
ssh 10.0.0.112 'pkill -9 java'

echo "Connecting to injector  machine and turning off everything"
ssh 10.0.0.110 'pkill -9 java'

echo "Collecting files from sink machine"
scp 10.0.0.112:/home/vincenzo/bes_flink/data_donotversion/output_rate.csv ${EXP_FOLDER}
scp 10.0.0.112:/home/vincenzo/bes_flink/data_donotversion/output_latency.csv ${EXP_FOLDER}
scp 10.0.0.112:/home/vincenzo/bes_flink/data_donotversion/sink.log ${EXP_FOLDER}

echo "Collecting files from injector machine"
scp 10.0.0.110:/home/vincenzo/bes_flink/data_donotversion/input_rate.csv ${EXP_FOLDER}
scp 10.0.0.110:/home/vincenzo/bes_flink/data_donotversion/injector.log ${EXP_FOLDER}

echo "Collecting files from slave machine"
scp 10.0.0.111:/tmp/throughput.csv ${EXP_FOLDER}

done
done
done
done
