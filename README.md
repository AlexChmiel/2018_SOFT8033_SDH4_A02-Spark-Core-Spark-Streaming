"# 2018_SOFT8033_SDH4_A02_AleksanderChmiel-Private" 

BACKGROUND

MongoDB is perhaps the most popular NoSQL database. Its documentation includes an example dataset with info of 25359 restaurants of New York City. The dataset, available at  https://raw.githubusercontent.com/mongodb/docs-assets/primer-dataset/primer-dataset.json, consists in a single file with a JSON object per line (representing the info of the restaurant).  
 
 

GOAL.  
 
1. Given the entire dataset (16 files), aggregate the following info per type of cuisine C  Total amount of reviews.   Total amount of negative reviews.   Total amount of points. To compute this, the points of each positive review are count as +points and the ones each negative are count as -points.  
 
The RDD is of type: (String, (String, int, int, int))         Example: (u'Donuts', (16, 0, 137)) 
 
2. Compute the average amount of reviews per type of cuisine  A There are many ways of doing this, but perhaps the simplest is to compute the total amount of reviews and divide it by the total amount of cuisines. 
 
The VAL is of type: float    Example: 5.25 
 
3. Given:  C – Result from point 1.  A – Result from point 2.  percentage_f - Represents the percentage of negative reviews we allow (e.g., 5 for 5%). 
 
 Compute C’ by removing the RDD entries in C not satisfying both:  i) Total amount of reviews >= A. ii) Percentage of bad reviews < percentage_f. 
 
4. Given C’, sort it by decreasing order in their average points per review.  
 
The RDD is of type: (String, (String, int, int, int, float))  Example: (u'Donuts', (16, 0, 137, 8.5625)) 
