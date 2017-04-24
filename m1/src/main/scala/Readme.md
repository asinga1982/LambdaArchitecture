Folder contect description:
---------------------------

batch: Has 3 files - 	
	batchJob.scala -> Code for bacth processing layer
	CreateCassTables.scala -> Code for creating Cassandra tables
	fetchCassTables.scala -> To retrieve data from Cassandra tables (to verify output) 

clickstream: Has code for genrating logs and writing them to Kafka topic: weblogs-text

config: Has variables that are read lazily

domain: Has definition for frequently used objects

functions: Has functions that are called from batch and online layers

streaming: Code for streaming layer

utils: Code for checkpointing and creating Spark and SQL contexts
