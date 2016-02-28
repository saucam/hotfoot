# hotfoot
WIP

Running:
./bin/spark-submit --class "com.guavus.hotfoot.Hotfoot" --jars --master local --driver-memory 64g --executor-memory 8g --executor-cores 4 --num-executors 24 /path/to/assemblyjar/hotfoot-assembly-1.0-SNAPSHOT.jar --schema-file sc.txt
