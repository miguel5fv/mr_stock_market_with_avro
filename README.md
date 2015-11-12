For this example I've tried three new aspects:

1. Use **Avro** to manage the data input and data output.
2. Use the **Combiner** step of MapReduces
3. Use new Java MapReduce API

I used the Avro Version 1.7.5, and it is usesful because anyone could access to that data (not only from Hadoop, but outside) using a client Avro with the Schema defined into the file *stock_market_schema.avsc*

The Combiner is not necesary, but I have used it because I'd like to explore that MapReduce Framework option.



