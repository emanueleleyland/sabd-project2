import boto3
import time

from flinkConfig import launch_flink_cluster
from subnets import createSubnets
from elasticIp import createIps
from securityGroup import createKafkaSecurityGroup, createZookeeperSecurityGroup, create_redis_security_group
from internetGateway import createInternetGateway
from routeTable import createRouteTable
from autoScalingGroup import createZookeeperAsgConfig, createKafkaAsgConfig, createKafkaAsg, createZookeeperAsg
from dns import create_kafka_dns_record
from redisConfig import create_redis_configuration, create_subnet_group


def create_eni(ip, sg_id, i):
    return subnets[i].create_network_interface(
        Groups=[
            sg_id,
        ],
        PrivateIpAddress=ip
    )


resources = {}

ec2 = boto3.resource('ec2')
ec2_client = boto3.client('ec2')
vpc = ec2.create_vpc(CidrBlock='10.0.0.0/16')
ec2_client.modify_vpc_attribute(

    EnableDnsSupport={
        'Value': True
    },
    VpcId=vpc.id
)
ec2_client.modify_vpc_attribute(

    EnableDnsHostnames={
        'Value': True
    },
    VpcId=vpc.id
)
vpc.create_tags(Tags=[{"Key": "Name", "Value": "vpc"}])
vpc.wait_until_available()
print(vpc.id)
resources['vpc'] = []
resources['vpc'].append({
    'id': vpc.id
})

# create subnets
subnets = []
createSubnets(ec2, subnets, vpc)

# Create EIPs
eips = []
createIps(eips, ec2_client, resources)

# Create sec group kafka
sec_group_kafka = createKafkaSecurityGroup(ec2, vpc)

# Create sec group zookeeper
sec_group_zookeeper = createZookeeperSecurityGroup(ec2, vpc)

# Create redis sec group
sec_group_redis = create_redis_security_group(ec2, vpc)

# Create Elastic Network Interfaces for zookeeper instances
eni_zk = [create_eni('10.0.0.11', sec_group_zookeeper.id, 0), create_eni('10.0.1.11', sec_group_zookeeper.id, 1),
          create_eni('10.0.2.11', sec_group_zookeeper.id, 2)]

# create then attach internet gateway
ig = createInternetGateway(ec2, vpc)

# create a route table and a public route
route_table = createRouteTable(vpc, ig)

# associate the route table with the subnets
for i in range(3):
    route_table.associate_with_subnet(SubnetId=subnets[i].id)

asg_client = boto3.client('autoscaling')

# create launch config for zookeeper
createZookeeperAsgConfig(eni_zk, asg_client, sec_group_zookeeper, resources)

# Create kafka launch configuration
createKafkaAsgConfig(asg_client, sec_group_kafka, eips, resources)

# Create zookeeper autoscaling group configuration
createZookeeperAsg(asg_client, subnets, resources)

# Create kafka autoscaling group configuration
createKafkaAsg(asg_client, subnets, resources)

# Create DNS records
route53_client = boto3.client('route53')
create_kafka_dns_record(eips, route53_client)
time.sleep(5)

# Create flink cluster
launch_flink_cluster(subnets, vpc)


# Create redis cluster
redis_client = boto3.client('elasticache')
subnet_group = create_subnet_group(redis_client, subnets)
create_redis_configuration(redis_client, subnet_group['CacheSubnetGroup']['CacheSubnetGroupName'], route53_client, sec_group_redis)
