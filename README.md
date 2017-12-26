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
* Use command `spark-submit --master local[2] --class com.github.lyon1982.BigDataExercise.BigDataExerciseApp target/scala-2.11/bigdataexercise_2.11-0.0.1.jar` to run the job with 2 local thread
* You can find the expected datasets from `/vagrant_app/data/ds1`, `/vagrant_app/data/ds2` and `/vagrant_app/data/ds3`

## Measure the Job

* Use command `spark-submit --master local[2] --packages ch.cern.sparkmeasure:spark-measure_2.11:0.11 --class com.github.lyon1982.BigDataExercise.BigDataExerciseAppWithMeasure target/scala-2.11/bigdataexercise_2.11-0.0.1.jar` to measure the performance