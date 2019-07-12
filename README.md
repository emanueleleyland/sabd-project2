# SABD_PROJECT_2

## Index

- [Description](#description)
- [Local deployment](#local-deployment)
- [Cloud deployment](#cloud-deployment)


## Description
The project is divided in 4 main directory:
* docker: scripts for local deployment
* aws: scripts for AWS deployment and performance evaluation
* flink: actual Flink core application
* kafka_streams: Kafka streams core application
* tuple-sender: comments traffic generator

## Local deployment
To deploy architecture locally you can use Docker. Startup scripts are available in `/docker` directory. You can choose in `start-all.sh` script which kafka deploy mode you want to use (SINGLE or CLUSTER)
Once all container has been started it is possible to connect to Apache Flink user iterface on address localhost:8081 to submit the `.JAR` file.

## Cloud deployment
Scripts are available in `/aws` directory. Install awscli and configure it with command `aws configure`. You need to install boto3 tool via `pip` to run scripts. 
Configure the scripts specifying the private .pem key to connect to you instances and your couple of `ACCESS_KEY_ID` and ACCESS_SECRET_KEY
Once you configured the scripts start `main.py` program to run the entire architecture, at some time it has to be necessary to insert redis endpoint in the input command line of the script.
To configure Flink cluster it is necessary to build the jar file with dependencies running:
```
mvn clean package
```
To run the application submit a new phase to EMR cluster as reported at the following link: https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/flink-jobs.html
If you want to monitor your application and you want to access to Flink User Interface you have to redirect via ssh tunnel typing in the terminal: 
```
ssh -i key.pem -C -D 8157 hadoop@master.public.dns
```
