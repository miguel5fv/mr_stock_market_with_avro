For this example I've tried three new aspects:

1. Use **Avro** to manage the data input and data output.
2. Use the **Combiner** step of MapReduces
3. Use new Java MapReduce API

I used the Avro Version 1.7.5, and it is usesful because anyone could access to that data (not only from Hadoop, but outside) using a client Avro with the Schema defined into the file *stock_market_schema.avsc*

The Combiner step could be ommited in this example, but I have decided to use it because I'd like to explore that MapReduce Framework option.

Firstly you have to create the *generated avro code* using this command:

`java -jar lib/avro-tools-1.7.5.jar compile schema stock_market_schema.avsc .`

And then compile the data in JSON format to Avro binary:

```
java -jar lib/avro-tools-1.7.5.jar fromjson stock_market_prices.json --schema-file stock_market_schema.avsc > prices.avro
hadoop fs -put prices.avro prices.avro
```

Then, to execute it you first needs to compile java class:

`javac -classpath $HADOOP_HOME/hadoop-core.jar:lib/avro-tools-1.7.5.jar:lib/avro-mapred-1.7.5.jar *.java`

Pack at them into a JAR file:

`jar cvf StockMarket.jar *.class`

And then add and execute the jar using Hadoop. In this case we use a command called -libsjar where indicate the external libs, like JSON:

```
export LIBJARS=lib/avro-tools-1.7.5.jar,lib/avro-mapred-1.7.5.jar
hadoop jar program_count.jar StockMarketDriver libjars ${LIBJARS} prices.avro output
```





