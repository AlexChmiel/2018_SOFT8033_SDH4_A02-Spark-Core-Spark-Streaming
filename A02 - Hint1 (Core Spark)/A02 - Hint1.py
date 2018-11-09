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
import sys
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
def my_map_filter(line, average_per_cuisine):
  cuisine = line[0]
  num_of_reviews = line[1][0]
  num_negative_reviews = line[1][1]
  num_points = line[1][2]
  if(num_negative_reviews != 0):
    percentage_negative = (num_negative_reviews / num_of_reviews) * 100.0 
  else:
    percentage_negative=0
  if (num_of_reviews <= average_per_cuisine or percentage_negative >= percentage_f):
    return ""
  else:
    return (cuisine, (num_of_reviews, num_negative_reviews, num_points, (num_points/num_of_reviews)))
  
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  
    # Load the dataset.
    datasetRDD = sc.textFile(dataset_dir)
    
    # Load json as python dictionary.
    pythonDictionary = datasetRDD.map(lambda x: json.loads(x))
    
    # Map by (cuisine,(num_of_reviews, num_of_negative_reviews)).
    mappedRDD = pythonDictionary.map(my_mapper)
     
    # C - Reduce by key.. output = (u'Hamburgers', (1676, 107, 11190)).
    # Caching this RDD because we will be reusing it later for part 3.
    reducedRDD = mappedRDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] +y[2])).cache()
    
    # Get total number of cuisines.
    num_cuisines = reducedRDD.count()
    
    # Map by reviews.
    average_cuisineRDD = reducedRDD.map(lambda f: f[1][0])
    
    # Reduce to get total reviews.
    total_num_cuisines = average_cuisineRDD.reduce(lambda x , y : x + y)
   
    # A- Calculate average per cuisine.
    average_per_cuisine = (total_num_cuisines/num_cuisines)

    # C' - remove RDD entries that don't meet 
    # i) total amount reviews > average_per_cuisine
    # ii) percentage of bad reviews <  percentage_f
    filteredRDD = reducedRDD.map(lambda k: my_map_filter(k, average_per_cuisine))
    
    # Remove empty RDD's
    cleanedRDD = filteredRDD.filter(lambda x: x is not None).filter(lambda x: x != "")
    
    # Sort by points in descending order
    sortedRDD = cleanedRDD.sortBy(lambda x: -x[1][2])
    
    # Print the results
    #for each in cleanedRDD.collect():
    #  print(each)
    sortedRDD.saveAsTextFile(result_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
