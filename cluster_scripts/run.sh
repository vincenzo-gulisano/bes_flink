
echo "Connecting to sink machine and starting sink"
ssh 10.46.0.119 '/home/odroid/bes_flink/cluster_scripts/sink.sh'

echo "Done..."
sleep 10

echo "Connecting to sink machine and turning off everything"
ssh 10.46.0.119 'pkill -9 java'
