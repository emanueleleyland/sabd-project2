def createKafkaSecurityGroup(ec2, vpc):
    sec_group_kafka = ec2.create_security_group(
        GroupName='kafka', Description='kafka sec group', VpcId=vpc.id)
    sec_group_kafka.authorize_ingress(
        IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 9092, 'ToPort': 9092, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]

    )
    print(sec_group_kafka.id)
    return sec_group_kafka



def createZookeeperSecurityGroup(ec2, vpc):
    sec_group_zookeeper = ec2.create_security_group(
        GroupName='zookeeper', Description='zookeeper', VpcId=vpc.id)
    sec_group_zookeeper.authorize_ingress(
        IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 2181, 'ToPort': 2181, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 2888, 'ToPort': 2888, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 3888, 'ToPort': 3888, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]
    )
    print(sec_group_zookeeper.id)
    return sec_group_zookeeper



def create_redis_security_group(ec2, vpc):
    sec_group_redis = ec2.create_security_group(
        GroupName='redis', Description='redis', VpcId=vpc.id)
    sec_group_redis.authorize_ingress(
        IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 6379, 'ToPort': 6379, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]
    )
    print(sec_group_redis.id)
    return sec_group_redis