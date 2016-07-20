cd ~/bes_flink
git pull
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-armhf/jre/
mvn package -Pbuild-jar
cp target/bes_flink-0.0.1-SNAPSHOT.jar ~/bes/
