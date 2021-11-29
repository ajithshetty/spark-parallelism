# SparkParallelism

What on earth does “Parallelising the parallel jobs” mean??
Without going in depth, On a layman term,


Spark creates the DAG or the Lineage based on the sequence we have created the RDD, applied transformations and actions.


It applies the Catalyst optimiser on the dataframe or dataset to tune your queries. but what it doesn’t do is, running your function in parallel to each other.


We always tend to think that the Spark is a framework which splits your jobs into tasks and stages and runs in parallel.


In a way it is 100% true. But not in the way what we are going to discuss below.


Lets say that I have 10 tables for which I need to apply the same function, eg. count, count the number of nulls, print the top rows, etc.


So in here If i submit the job for 10 tables will it run parallel, since these 10 tables are independent of each other ???


Spark is smart enough to figure out the dependency and run things parallel, isn’t it?

DEMO: https://ajithshetty28.medium.com/?p=77b819314d5a
