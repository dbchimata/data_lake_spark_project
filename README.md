# Project 3: STEDI Human Balance Analytics
--------------------------
In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

## Project Details

The STEDI Team has been hard at work developing a hardware **STEDI Step Trainer** that:
* trains the user to do a STEDI balance exercise
* has sensors on the device that collect data to train a machine-learning algorithm to detect steps
* has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. **Only these customers' Step Trainer and accelerometer data should be used in the training data for the machine learning model.**

## Project Summary

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.


## Project Data

This project involves working with three JSON data sources provided by STEDI for the Step Trainer. The data can be downloaded or extracted from their respective public S3 bucket locations.

**Customer Records (from fulfillment and the STEDI website):**

Data Download URL: Data Download URL
AWS S3 Bucket URI: s3://cd0030bucket/customers/
Fields included: serialnumber, sharewithpublicasofdate, birthday, registrationdate, sharewithresearchasofdate, customername, email, lastupdatedate, phone, sharewithfriendsasofdate
Step Trainer Records (data from the motion sensor):

Data Download URL: Data Download URL
AWS S3 Bucket URI: s3://cd0030bucket/step_trainer/
Fields included: sensorReadingTime, serialNumber, distanceFromObject
Accelerometer Records (from the mobile app):

Data Download URL: Data Download URL
AWS S3 Bucket URI: s3://cd0030bucket/accelerometer/
Fields included: timeStamp, user, x, y, z

## Project Instructions

To complete this project, you will utilize AWS Glue, AWS S3, Python, and Spark to build a lakehouse solution in AWS that meets the requirements from the STEDI data scientists. 

**Requirements**

Create S3 directories for landing zones: customer_landing, step_trainer_landing, and accelerometer_landing. Copy the respective data into these directories as a starting point.

Create Glue tables: customer_landing and accelerometer_landing to store the data in a semi-structured format. Share the scripts customer_landing.sql and accelerometer_landing.sql in your git repository.

Query the tables using Athena and capture screenshots of the resulting data. Save the screenshots as customer_landing.png and accelerometer_landing.png.

**Create two AWS Glue Jobs to perform the following tasks:**

Sanitize the Customer data from the Website (Landing Zone) and store only the Customer Records who agreed to share their data for research purposes in a Glue Table named customer_trusted.
Sanitize the Accelerometer data from the Mobile App (Landing Zone) and store only the Accelerometer Readings from customers who agreed to share their data for research purposes in a Glue Table named accelerometer_trusted.
Verify the success of the Glue jobs and ensure that the customer_trusted table contains only Customer Records from people who agreed to share their data. Query the customer_trusted table with Athena and capture a screenshot of the data. Save the screenshot as customer_trusted.png.

Create a Glue job that sanitizes the Customer data (Trusted Zone) and creates a Glue Table named customers_curated. Include only customers who have accelerometer data and have agreed to share their data for research.

**Create two Glue Studio jobs to perform the following tasks:**

Read the Step Trainer IoT data stream from S3 and populate a Trusted Zone Glue Table named step_trainer_trusted. Include Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
Create an aggregated table that combines the Step Trainer Readings with the associated accelerometer reading data for the same timestamp. Include only data from customers who have agreed to share their data and save it as a Glue Table named machine_learning_curated.