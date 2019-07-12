import json
import boto3

with open('resources.json') as f:
    resources = json.load(f)

autoscaling_group_client = boto3.client('autoscaling')
for i in range(resources['kafkaAsg'].length):
    autoscaling_group_client.delete_auto_scaling_group(resources['kafkaAsg']['autoscaling-kafka-' + chr(97 + i)])
for i in range(resources['zkAsg'].length):
    autoscaling_group_client.delete_auto_scaling_group(resources['zkAsg']['autoscaling-zookeeper-' + str(i)])
for i in range(resources['kafkaLaunchConfig'].length):
    autoscaling_group_client.delete_launch_configuration(resources['kafkaLaunchConfig']['launch_configuration_kafka_' + str(i)])
for i in range(resources['zkLaunchConfig'].length):
    autoscaling_group_client.delete_launch_configuration(resources['zkLaunchConfig']['launch_configuration_zookeeper_' + str(i)])

ec2_client = boto3.client('ec2')
# for i in range(resources['eip'].length):
#     ec2_client.
