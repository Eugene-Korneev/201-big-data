## Execution plans analysis

### Main bottlenecks
The main weak and slow place in every query is reading from AVRO and Kafka topic as external tables - 
    all rows should be read and deserialized before they can be processed.

### What to improve
It's better to copy all data from AVRO and Kafka (external tables) to respective hive tables stored as ORC 
    and to organize an incremental update of these tables in case of consistently incoming data 
    (from streams / batches).
Additionally, to optimize queries to source ORC tables the partitioning by date and the bucketing by id 
    should be applied.
