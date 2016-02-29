# hotfoot
WIP

Running:
./bin/spark-submit --class "com.guavus.hotfoot.Hotfoot" --master yarn-client --driver-memory 64g --executor-memory 8g --executor-cores 4 --num-executors 2 ../hotfoot/hotfoot-assembly-1.0-SNAPSHOT.jar --schema-file ../hotfoot/schema.json --output-path /data/hotfoot/output


TODO:
- codegen to eliminate iterator flatmap
- generators for values
