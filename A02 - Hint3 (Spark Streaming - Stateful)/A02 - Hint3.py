# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------
import time
from pyspark.streaming import StreamingContext
import json
from __future__ import division

# ------------------------------------------
# FUNCTION my_mapper
# ------------------------------------------
def my_mapper(line):
  # 1. Set up variables for the following structure: (cuisine, (num_of_reviews, num_negative_reviews, num_points))
  review = line["evaluation"]
  points = int(line["points"])
  cuisine = line["cuisine"]
  num_of_reviews = 1 # This is going to be 1 as each line read in is a review.
  num_negative_reviews = 0
  num_points = 0
  
  # 2. Check if review is negative, if it is substract points. If positive, add points.
  if(review.lower() == "negative"):
    num_negative_reviews += 1
    num_points = num_points - points
  elif(review.lower() == "positive"):
    num_points = num_points + points
  
  # 3. Return the tuple.
  return (cuisine, (num_of_reviews, num_negative_reviews, num_points))


# ------------------------------------------
# FUNCTION my_map_filter
# ------------------------------------------
def my_map_filter(line):
  cuisine = line[0]
  num_of_reviews = line[1][0]
  num_negative_reviews = line[1][1]
  num_points = line[1][2]
  total_reviews = line[1][3]
  total_cuisines = line[1][4]
  average_per_cuisine = total_reviews/total_cuisines
  
  if(num_negative_reviews != 0):
    percentage_negative = (num_negative_reviews/num_of_reviews)*100.0
  else:
    percentage_negative=0
    
  if (num_of_reviews <= average_per_cuisine or percentage_negative >= percentage_f):
    return ""
  else:
    return (cuisine, (num_of_reviews, num_negative_reviews, num_points, num_points/num_of_reviews))
  
# ------------------------------------------
# FUNCTION my_fake_mapper
# ------------------------------------------  
def my_fake_mapper(line):
  cuisine = line[0]
  num_reviews = line[1][0]
  num_negative_reviews = line[1][1]
  num_points = line[1][2]
  
  return ("fake", (cuisine, num_reviews, num_negative_reviews, num_points)) 


# ------------------------------------------
# FUNCTION my_combineMapper
# ------------------------------------------
def my_combineMapper(line):
  fake = line[0]
  cuisine = line[1][0][0]
  
  num_reviews = line[1][0][1]
  num_negative_reviews = line[1][0][2]
  num_points = line[1][0][3]
  
  total_reviews = line[1][1][0]
  total_cuisines = line[1][1][1]
  
  return (cuisine, (num_reviews, num_negative_reviews, num_points, total_reviews, total_cuisines))
  

# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration):
    inputDStream = ssc.textFileStream(monitoring_dir)
    
    # Create a window of 4 RDD's
    windowDStream = inputDStream.window(window_duration * time_step_interval, sliding_duration * time_step_interval)

    # Load json as python dictionary.
    pythonDictionary = windowDStream.map(lambda x: json.loads(x))
    
    # Map by (cuisine,(num_of_reviews, num_of_negative_reviews)).
    mappedRDD = pythonDictionary.map(my_mapper)
     
    # C - Reduce by key.. output = (u'Hamburgers', (1676, 107, 11190)).
    # Caching this RDD because we will be reusing it later for part 3.
    reducedRDD = mappedRDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] +y[2])).cache()
    
    # Map by reviews.
    average_cuisineRDD = reducedRDD.map(my_fake_mapper)

    #2.1 Reduce to get total reviews.
    #total_num_reviews = average_cuisineRDD.reduce(lambda x, y : x + y) #2.1
    total_num_reviews = windowDStream.count()
  
    #2.2 Get total number of cuisines.
    total_num_cuisines = reducedRDD.count() #2.2
  
    #2.3 map of (fake, total_num_reviews)
    fakeNumReviewsRDD = total_num_reviews.map(lambda x: ("fake", x))
    
    #2.4 map of (fake, total_num_cuisines)
    fakeNumCuisinesRDD = total_num_cuisines.map(lambda x: ("fake",x))
    
    #2.5 JOIN total_num_reviews and total_num_cuisines together
    joinedReviewsCuisinesRDD = fakeNumReviewsRDD.join(fakeNumCuisinesRDD)
    
    # Join average_cuisineRDD and joinedReviewsCuisinesRDD
    joinedReviewsCuisinesAndPreviousRDD = average_cuisineRDD.join(joinedReviewsCuisinesRDD)
    
    # Map the RDD to put it into 1 tuple
    combinedRDD = joinedReviewsCuisinesAndPreviousRDD.map(my_combineMapper)

    # C' - remove RDD entries that don't meet 
    # i) total amount reviews > average_per_cuisine
    # ii) percentage of bad reviews <  percentage_f
    filteredRDD = combinedRDD.map(my_map_filter)
    
    # Remove empty lines in RDD's
    cleanedRDD = filteredRDD.filter(lambda x: x is not None).filter(lambda x: x != "")
    
    # Sort by points in descending order
    sortedRDD = cleanedRDD.transform( lambda rdd : rdd.sortBy(lambda x: x[1][3],ascending=False))
    
    # Save the results to textfile
    sortedRDD.saveAsTextFiles(result_dir)
    #sortedRDD.pprint()
    
# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(monitoring_dir,
               result_dir,
               max_micro_batches,
               time_step_interval,
               percentage_f,
               window_duration,
               sliding_duration):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, percentage_f, window_duration, sliding_duration)

    # 4. We return the ssc configured and modelled.
    return ssc


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)
        if verbose == True:
            print(file_name)

        # 3.2. We look for the pattern name= to remove all useless info from the start
        lb_index = file_name.index("name=u'")
        file_name = file_name[(lb_index + 7):]

        # 3.3. We look for the pattern ') to remove all useless info from the end
        ub_index = file_name.index("',")
        file_name = file_name[:ub_index]

        # 3.4. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(source_dir, verbose)

    # 2. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 2.1. We copy the file from source_dir to dataset_dir
        dbutils.fs.cp(source_dir + file, monitoring_dir + file, False)

        # 2.2. We wait the desired transfer_interval
        time.sleep(time_step_interval)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = StreamingContext.getActiveOrCreate(checkpoint_dir,
                                             lambda: create_ssc(monitoring_dir,
                                                                result_dir,
                                                                max_micro_batches,
                                                                time_step_interval,
                                                                percentage_f,
                                                                window_duration,
                                                                sliding_duration
                                                                )
                                             )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    if (race_conditions_extra_delay == True):
        time.sleep((sliding_duration - 1) * time_step_interval)

        # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 6. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input source folder (static dataset),
    # monitoring folder (dynamic dataset simulation) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    monitoring_dir = "/FileStore/tables/A02/my_monitoring/"
    checkpoint_dir = "/FileStore/tables/A02/my_checkpoint/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 16

    # 3. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 10

    # 4. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 5. We configure verbosity during the program run
    verbose = False

    # 6. Extra input arguments
    percentage_f = 10
    window_duration = 4
    sliding_duration = 4

    # 6.4. RACE Conditions: Discussed above. Basically, in which moment of the sliding_window do I want to start.
    # This performs an extra delay at the start of the file transferred to sync SparkContext with file transferrence.
    race_conditions_extra_delay = True

    # 7. We remove the monitoring and output directories
    dbutils.fs.rm(monitoring_dir, True)
    dbutils.fs.rm(result_dir, True)
    dbutils.fs.rm(checkpoint_dir, True)

    # 8. We re-create them again
    dbutils.fs.mkdirs(monitoring_dir)
    dbutils.fs.mkdirs(result_dir)
    dbutils.fs.mkdirs(checkpoint_dir)

    # 9. We call to my_main
    my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay
            )
