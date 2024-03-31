---
jupyter:
  kernelspec:
    display_name: causalml-py38
    language: python
    name: python3
  language_info:
    codemirror_mode:
      name: ipython
      version: 3
    file_extension: .py
    mimetype: text/x-python
    name: python
    nbconvert_exporter: python
    pygments_lexer: ipython3
    version: 3.8.18
  nbformat: 4
  nbformat_minor: 2
---

::: {.cell .markdown}
# 1. Setup {#1-setup}
:::

::: {.cell .code execution_count="1"}
``` python
! pip show confluent-kafka
```

::: {.output .stream .stdout}
    Name: confluent-kafka
    Version: 2.3.0
    Summary: Confluent's Python client for Apache Kafka
    Home-page: https://github.com/confluentinc/confluent-kafka-python
    Author: Confluent Inc
    Author-email: support@confluent.io
    License: 
    Location: /Users/sheidamajidi/anaconda3/envs/causalml-py38/lib/python3.8/site-packages
    Requires: 
    Required-by: 
:::
:::

::: {.cell .code execution_count="1"}
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
```
:::

::: {.cell .code execution_count="5"}
``` python
spark = SparkSession.builder \
        .appName("KafkaIntegrationExample") \
        .getOrCreate()
```

::: {.output .stream .stderr}
    24/03/30 22:20:13 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
:::
:::

::: {.cell .markdown}
# 2. Batch Processing {#2-batch-processing}
:::

::: {.cell .markdown}
## 2.1. Load topic data from Confluent in batch mode {#21-load-topic-data-from-confluent-in-batch-mode}
:::

::: {.cell .code execution_count="3"}
``` python
from pyspark.sql import SparkSession, functions
topic_name = "test-topic"
bootstrap_servers = "server"
df_kafka = spark.read.format("kafka")\
    .option("kafka.bootstrap.servers", bootstrap_servers)\
    .option("subscribe", topic_name)\
    .option("kafka.security.protocol","SASL_SSL")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user name\" password=\"pass\";")\
    .load()

display(df_kafka)
```

::: {.output .display_data}
    DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
:::
:::

::: {.cell .code execution_count="5"}
``` python
df_kafka
```

::: {.output .execute_result execution_count="5"}
    DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
:::
:::

::: {.cell .markdown}
## 2.2. Write a Kafka sink for batch queries {#22-write-a-kafka-sink-for-batch-queries}
:::

::: {.cell .markdown}
### 2.2.1. Create sample data {#221-create-sample-data}
:::

::: {.cell .code execution_count="26"}
``` python
# import pyspark class Row from module sql
from pyspark.sql import *
```
:::

::: {.cell .code execution_count="27"}
``` python
sample_data = Row("key", "value", "topic")

samples = [
    sample_data('1', '{"name": "Jane Doe", "age": 29, "email": "jane.doe@example.com"}', "test-topic"),
    sample_data('2', '{"name": "John Smith", "age": 34, "email": "john.smith@example.com"}', "test-topic"),
    sample_data('3', '{"name": "Emily Jones", "age": 23, "email": "emily.jones@example.com"}', "test-topic"),
    sample_data('4', '{"name": "Michael Brown", "age": 45, "email": "michael.brown@example.com"}', "test-topic"),
    sample_data('5', '{"name": "Linda White", "age": 52, "email": "linda.white@example.com"}', "test-topic"),
    sample_data('6', '{"name": "David Harris", "age": 37, "email": "david.harris@example.com"}', "test-topic"),
    sample_data('7', '{"name": "Jessica Clark", "age": 28, "email": "jessica.clark@example.com"}', "test-topic"),
    sample_data('8', '{"name": "Daniel Lewis", "age": 43, "email": "daniel.lewis@example.com"}', "test-topic"),
    sample_data('9', '{"name": "Laura Allen", "age": 19, "email": "laura.allen@example.com"}', "test-topic"),
    sample_data('10', '{"name": "Kevin Walker", "age": 56, "email": "kevin.walker@example.com"}', "test-topic"),
    sample_data('11', '{"name": "Sarah Hall", "age": 33, "email": "sarah.hall@example.com"}', "test-topic"),
    sample_data('12', '{"name": "Brian Young", "age": 26, "email": "brian.young@example.com"}', "test-topic"),
    sample_data('13', '{"name": "Nancy King", "age": 49, "email": "nancy.king@example.com"}', "test-topic"),
    sample_data('14', '{"name": "Paul Scott", "age": 38, "email": "paul.scott@example.com"}', "test-topic"),
    sample_data('15', '{"name": "Lisa Green", "age": 31, "email": "lisa.green@example.com"}', "test-topic"),
    sample_data('16', '{"name": "James Adams", "age": 22, "email": "james.adams@example.com"}', "test-topic"),
    sample_data('17', '{"name": "Sandra Thompson", "age": 46, "email": "sandra.thompson@example.com"}', "test-topic")
]

# for demonstration, I print the first sample
print(samples[0])
```

::: {.output .stream .stdout}
    Row(key='1', value='{"name": "Jane Doe", "age": 29, "email": "jane.doe@example.com"}', topic='test-topic')
:::
:::

::: {.cell .markdown}
### 2.2.2. Create a dataframe from sample data {#222-create-a-dataframe-from-sample-data}
:::

::: {.cell .code execution_count="30"}
``` python
# converting the list to a DataFrame
df = spark.createDataFrame(samples)
```
:::

::: {.cell .code execution_count="31"}
``` python
# df.show(truncate=False)
df.show()
```

::: {.output .stream .stdout}
    +---+--------------------+----------+
    |key|               value|     topic|
    +---+--------------------+----------+
    |  1|{"name": "Jane Do...|test-topic|
    |  2|{"name": "John Sm...|test-topic|
    |  3|{"name": "Emily J...|test-topic|
    |  4|{"name": "Michael...|test-topic|
    |  5|{"name": "Linda W...|test-topic|
    |  6|{"name": "David H...|test-topic|
    |  7|{"name": "Jessica...|test-topic|
    |  8|{"name": "Daniel ...|test-topic|
    |  9|{"name": "Laura A...|test-topic|
    | 10|{"name": "Kevin W...|test-topic|
    | 11|{"name": "Sarah H...|test-topic|
    | 12|{"name": "Brian Y...|test-topic|
    | 13|{"name": "Nancy K...|test-topic|
    | 14|{"name": "Paul Sc...|test-topic|
    | 15|{"name": "Lisa Gr...|test-topic|
    | 16|{"name": "James A...|test-topic|
    | 17|{"name": "Sandra ...|test-topic|
    +---+--------------------+----------+
:::
:::

::: {.cell .code execution_count="23"}
``` python
display(df)
```

::: {.output .display_data}
    DataFrame[customer_id: string, age: bigint, gender: string, email: string, total_spent: double]
:::
:::

::: {.cell .markdown}
### 2.2.3. Write data from a dataframe to a confluent kafka topic {#223-write-data-from-a-dataframe-to-a-confluent-kafka-topic}
:::

::: {.cell .code execution_count="39"}
``` python
# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", topic_name) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";") \
    .save()
```

::: {.output .stream .stderr}
                                                                                    
:::
:::

::: {.cell .markdown}
# 3. Stream Processing {#3-stream-processing}
:::

::: {.cell .markdown}
## 3.1. Read a stream from Kafka {#31-read-a-stream-from-kafka}
:::

::: {.cell .code execution_count="42"}
``` python
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers)\
    .option("subscribe", topic_name)\
    .option("kafka.security.protocol","SASL_SSL")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";")\
    .load() \

display(df_stream)
```

::: {.output .display_data}
    DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
:::
:::

::: {.cell .markdown}
## 3.2. Write a Kafka sink for streaming queries {#32-write-a-kafka-sink-for-streaming-queries}
:::

::: {.cell .code execution_count="45"}
``` python
# Write key-value data from a DataFrame to a specific Kafka topic specified in an option
ds = df_stream \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers)\
    .option("subscribe", topic_name)\
    .option("kafka.security.protocol","SASL_SSL")\
    .option("kafka.sasl.mechanism", "PLAIN")\
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";")\
  .option("topic", "databricks_test") \
  .option("checkpointLocation", "/Users/sheidamajidi/Desktop") \
  .start()
```

::: {.output .stream .stderr}
    24/03/31 01:33:46 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
:::

::: {.output .stream .stderr}
    24/03/31 01:33:46 WARN KafkaOffsetReaderAdmin: Error in attempt 1 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 01:33:47 WARN KafkaOffsetReaderAdmin: Error in attempt 2 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 01:33:48 WARN KafkaOffsetReaderAdmin: Error in attempt 3 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 01:33:49 ERROR MicroBatchExecution: Query [id = 5015ae47-4877-48d4-b24a-baf2c96b71a8, runId = 3a9882b5-13a3-450e-8419-73465c34f591] terminated with error
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
:::
:::

::: {.cell .code execution_count="48"}
``` python
# saving to DBFS
df_stream \
  .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
  .writeStream \
  .format("json") \
  .option("path", "/Users/sheidamajidi/Desktop") \
  .option("checkpointLocation", "/Users/sheidamajidi/Desktop") \
  .start()
```

::: {.output .stream .stderr}
    24/03/31 02:07:39 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
:::

::: {.output .execute_result execution_count="48"}
    <pyspark.sql.streaming.query.StreamingQuery at 0x14e10c610>
:::

::: {.output .stream .stderr}
    24/03/31 02:07:39 WARN KafkaOffsetReaderAdmin: Error in attempt 1 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:07:40 WARN KafkaOffsetReaderAdmin: Error in attempt 2 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:07:41 WARN KafkaOffsetReaderAdmin: Error in attempt 3 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:07:42 ERROR MicroBatchExecution: Query [id = 5015ae47-4877-48d4-b24a-baf2c96b71a8, runId = 8884b219-b7e5-4bc3-9451-3e7844d3f62f] terminated with error
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
:::
:::

::: {.cell .code execution_count="47"}
``` python
ds = df_stream \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_servers) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";") \
  .option("topic", "databricks_test") \
  .option("checkpointLocation", "/Users/sheidamajidi/Desktop") \
  .start()
```

::: {.output .stream .stderr}
    24/03/31 02:05:50 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
:::

::: {.output .stream .stderr}
    24/03/31 02:05:50 WARN KafkaOffsetReaderAdmin: Error in attempt 1 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:05:51 WARN KafkaOffsetReaderAdmin: Error in attempt 2 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:05:52 WARN KafkaOffsetReaderAdmin: Error in attempt 3 getting Kafka offsets: 
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
    24/03/31 02:05:53 ERROR MicroBatchExecution: Query [id = 5015ae47-4877-48d4-b24a-baf2c96b71a8, runId = 50395463-ecf9-45c7-8e00-c517704bba53] terminated with error
    org.apache.kafka.common.KafkaException: Failed to create new KafkaAdminClient
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:551)
    	at org.apache.kafka.clients.admin.Admin.create(Admin.java:144)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin(ConsumerStrategy.scala:50)
    	at org.apache.spark.sql.kafka010.ConsumerStrategy.createAdmin$(ConsumerStrategy.scala:47)
    	at org.apache.spark.sql.kafka010.SubscribeStrategy.createAdmin(ConsumerStrategy.scala:102)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.admin(KafkaOffsetReaderAdmin.scala:70)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.$anonfun$partitionsAssignedToAdmin$1(KafkaOffsetReaderAdmin.scala:499)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.withRetries(KafkaOffsetReaderAdmin.scala:518)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.partitionsAssignedToAdmin(KafkaOffsetReaderAdmin.scala:498)
    	at org.apache.spark.sql.kafka010.KafkaOffsetReaderAdmin.fetchLatestOffsets(KafkaOffsetReaderAdmin.scala:297)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.$anonfun$getOrCreateInitialPartitionOffsets$1(KafkaMicroBatchStream.scala:246)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.getOrCreateInitialPartitionOffsets(KafkaMicroBatchStream.scala:241)
    	at org.apache.spark.sql.kafka010.KafkaMicroBatchStream.initialOffset(KafkaMicroBatchStream.scala:98)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$getStartOffset$2(MicroBatchExecution.scala:457)
    	at scala.Option.getOrElse(Option.scala:189)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.getStartOffset(MicroBatchExecution.scala:457)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$4(MicroBatchExecution.scala:491)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$2(MicroBatchExecution.scala:490)
    	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
    	at scala.collection.Iterator.foreach(Iterator.scala:943)
    	at scala.collection.Iterator.foreach$(Iterator.scala:943)
    	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
    	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
    	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
    	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
    	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
    	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$constructNextBatch$1(MicroBatchExecution.scala:479)
    	at scala.runtime.java8.JFunction0$mcZ$sp.apply(JFunction0$mcZ$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.withProgressLocked(MicroBatchExecution.scala:810)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.constructNextBatch(MicroBatchExecution.scala:475)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$2(MicroBatchExecution.scala:268)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken(ProgressReporter.scala:427)
    	at org.apache.spark.sql.execution.streaming.ProgressReporter.reportTimeTaken$(ProgressReporter.scala:425)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.reportTimeTaken(StreamExecution.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runActivatedStream$1(MicroBatchExecution.scala:249)
    	at org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor.execute(TriggerExecutor.scala:67)
    	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.runActivatedStream(MicroBatchExecution.scala:239)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.$anonfun$runStream$1(StreamExecution.scala:311)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
    	at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:289)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.$anonfun$run$1(StreamExecution.scala:211)
    	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
    	at org.apache.spark.JobArtifactSet$.withActiveJobArtifactState(JobArtifactSet.scala:94)
    	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:211)
    Caused by: org.apache.kafka.common.KafkaException: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:184)
    	at org.apache.kafka.common.network.ChannelBuilders.create(ChannelBuilders.java:192)
    	at org.apache.kafka.common.network.ChannelBuilders.clientChannelBuilder(ChannelBuilders.java:81)
    	at org.apache.kafka.clients.ClientUtils.createChannelBuilder(ClientUtils.java:105)
    	at org.apache.kafka.clients.admin.KafkaAdminClient.createInternal(KafkaAdminClient.java:522)
    	... 51 more
    Caused by: javax.security.auth.login.LoginException: No LoginModule found for kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
    	at java.base/javax.security.auth.login.LoginContext.invoke(LoginContext.java:731)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:672)
    	at java.base/javax.security.auth.login.LoginContext$4.run(LoginContext.java:670)
    	at java.base/java.security.AccessController.doPrivileged(Native Method)
    	at java.base/javax.security.auth.login.LoginContext.invokePriv(LoginContext.java:670)
    	at java.base/javax.security.auth.login.LoginContext.login(LoginContext.java:581)
    	at org.apache.kafka.common.security.authenticator.AbstractLogin.login(AbstractLogin.java:60)
    	at org.apache.kafka.common.security.authenticator.LoginManager.<init>(LoginManager.java:62)
    	at org.apache.kafka.common.security.authenticator.LoginManager.acquireLoginManager(LoginManager.java:105)
    	at org.apache.kafka.common.network.SaslChannelBuilder.configure(SaslChannelBuilder.java:170)
    	... 55 more
:::
:::
