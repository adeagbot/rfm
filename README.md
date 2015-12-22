# rfm
Recency Frequency Monetary Metric in Spark Scala

RFM (recency, frequency, monetary) analysis is a marketing technique used to determine quantitatively which customers are the best ones by examining how recently a customer has purchased (recency), how often they purchase (frequency), and how much the customer spends (monetary).



spark-submit --class main.Runner --master local[4] rfm.jar file:/home/myfolder/rfm.csv

Example of result

|userId| recency| frequency| duration 

657,2014-07-20T18:20:56.000Z,1570,11914000
270,2014-07-20T18:20:56.000Z,1557,12357000
671,2014-07-20T18:20:56.000Z,1554,11743000
2240,2014-07-20T18:20:56.000Z,1546,12163000
 ,2014-07-18T21:26:23.000Z,5712,48387000
2242,2014-07-18T21:26:23.000Z,1160,9567000
