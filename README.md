# Big Data Exercise

## Setup a Dev & Testing Environment

* Install VirtualBox following the instructions from this page: https://www.virtualbox.org/wiki/Downloads
* Install Vagrant following the instructions from this page: https://www.vagrantup.com/downloads.html
* Under `vagrant` folder run `vagrant up`
* When the VM is up and running, run command `vagrant ssh` to get into the VM console
* Go into `/vagrant_app/code` and run `sbt test` to run the unit tests to check if everything is working

## Run Job Against Real Data

* Go into `/vagrant_app/data` folder and run command `./download-from-s3.sh "<AccessKeyID>" "<AccessKeySecret>"` to retrieve the sample data from S3
* Go into `/vagrant_app/code` and run `sbt package` to create a jar package
* Use command `spark-submit --master local[2] --class com.github.lyon1982.BigDataExercise.BigDataExerciseApp target/scala-2.11/bigdataexercise_2.11-0.0.1.jar` to run the job with 2 local threads
* You may find the expected datasets from `/vagrant_app/data/ds1`, `/vagrant_app/data/ds2` and `/vagrant_app/data/ds3`

## Measure the Job

* Use command `spark-submit --master local[2] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11 --class com.github.lyon1982.BigDataExercise.BigDataExerciseAppWithMeasure target/scala-2.11/bigdataexercise_2.11-0.0.1.jar` to measure the performance

## Design & Choices

![BDE-Flow](https://github.com/Lyon1982/big-data-exercise/blob/master/docs/BDE-Flow.png?raw=true)



### Size of Data

I imagine the dataset could be 1000,000x bigger and made the design based on that surmise. 

### Preprocessing

For all three input datasets, there are three optional tasks need to be done in advance:

1. Prune: Remove unneeded columns to save resources.
2. Validate & Filter: Make sure the values from the needed columns are valid, filter any invalid records out.
3. Add Calculated Column: For customers a calculated Age column and for bookings a calculated  BookingStayInterval column are required.

So I use a builder strategy(under `com.github.lyon1982.BigDataExercise.builder` package) to make it open for future extension.

### 2 Major Jobs

GroupBy and Join need shuffle and could be expensive for big datasets. I split the work into two major jobs. One is hotel and booking related analysis and the other one is customer and booking related analysis. In this way, one job will only use the data from two datasets and get a better chance to do the work all in memory. You may find those two jobs in `com.github.lyon1982.BigDataExercise.jobs` package.

### Performance Measurement

I'm using an open source library [sparkMeasure](https://github.com/LucaCanali/sparkMeasure) to do the performance measurement rather than implement it with Listeners myself as I don't think I can do a better job than what it does in a short period of time.

## Business Value

We can find three import points as below:
First,stay length by city and country;
Second,stay length by age and gender;
Last,the interval between booking and stay date per customer gender,age and hotel country.

We can get the status of the stay length base on age,gender,city and country these four dimensions. Then could update service strategy to meet the requirement in real time.Such as announce special price discount for long stay in the city where most customers stay shortly.According to the last value, we also can update service strategy that provide some special offers to customers who booking more earlier in the country where most customers booking at short intervals,etc.
