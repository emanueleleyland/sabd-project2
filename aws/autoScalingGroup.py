from config import ACCESS_KEY_ID, ACCESS_SECRET_KEY, MIN_KAFKA_CLUSTER_SIZE, MAX_KAFKA_CLUSTER_SIZE, \
    DESIRED_KAFKA_CLUSTER_SIZE, AMI_SERVER, KEY_PEM, INSTANCE_TYPE_LARGE_DISK, INSTANCE_TYPE_MEDIUM_T, \
    INSTANCE_TYPE_XLARGE


def createZookeeperAsg(autoscaling_group_client, subnets, resources):
    resources['zkAsg'] = []
    for i in range(3):
        autoscaling_group_client.create_auto_scaling_group(
            VPCZoneIdentifier=subnets[i].id,
            AutoScalingGroupName='autoscaling-zookeeper-' + str(i),
            LaunchConfigurationName='launch_configuration_zookeeper_' + str(i),
            MinSize=1,
            MaxSize=1,
        )
        resources['zkAsg'].append({
            'id' + str(i): 'autoscaling-zookeeper-' + str(i)
        })


def createKafkaAsg(autoscaling_group_client, subnets, resources):
    resources['kafkaAsg'] = []
    for i in range(3):
        try:
            autoscaling_group_client.describe_auto_scaling_groups(AutoScalingGroupNames='autoscaling-kafka-' + chr(97 + i))
        except:
            autoscaling_group_client.create_auto_scaling_group(
                VPCZoneIdentifier=subnets[i].id,
                AutoScalingGroupName='autoscaling-kafka-' + chr(97 + i),
                LaunchConfigurationName='launch_configuration_kafka_' + str(i),
                MinSize=MIN_KAFKA_CLUSTER_SIZE,
                MaxSize=MAX_KAFKA_CLUSTER_SIZE,
                DesiredCapacity=DESIRED_KAFKA_CLUSTER_SIZE
            )
            resources['kafkaAsg'].append({
             'id' + str(i): 'autoscaling-kafka-' + chr(97 + i)
            })
            pass


def createZookeeperAsgConfig(eni_zk, autoscaling_group_client, sec_group_zookeeper, resources):
    user_data_zookeeper = [create_zk_user_data(eni_zk[0].id, '10.0.0.11'),
                           create_zk_user_data(eni_zk[1].id, '10.0.1.11'),
                           create_zk_user_data(eni_zk[2].id, '10.0.2.11')]
    resources['zkLaunchConfig'] = []
    # Create zookeeper launch configuration
    for i in range(0, len(user_data_zookeeper)):
        autoscaling_group_client.create_launch_configuration(
            LaunchConfigurationName='launch_configuration_zookeeper_' + str(i),
            ImageId=AMI_SERVER,
            UserData=user_data_zookeeper[i],
            SecurityGroups=[
                sec_group_zookeeper.id
            ],
            InstanceType=INSTANCE_TYPE_MEDIUM_T,
            AssociatePublicIpAddress=True,
            KeyName=KEY_PEM
        )
        resources['zkLaunchConfig'].append({
            'id' + str(i): 'launch_configuration_zookeeper_' + str(i)
        })

    print('zk launch config')


def createKafkaAsgConfig(autoscaling_group_client, sec_group_kafka, eips, resources):
    resources['kafkaLaunchConfig'] = []
    for i in range(3):
        user_data_kafka = create_kafka_user_data(eips[i]["AllocationId"])
        autoscaling_group_client.create_launch_configuration(
            LaunchConfigurationName='launch_configuration_kafka_' + str(i),
            ImageId=AMI_SERVER,
            UserData=user_data_kafka,
            SecurityGroups=[
                sec_group_kafka.id
            ],
            InstanceType=INSTANCE_TYPE_XLARGE,
            AssociatePublicIpAddress=True,
            KeyName=KEY_PEM,
            BlockDeviceMappings=[{
                'DeviceName': '/dev/sdb',
                'Ebs': {
                    'VolumeSize': 20
                }
            }]
        )
        resources['kafkaLaunchConfig'].append({
            'id' + str(i): 'launch_configuration_kafka_' + str(i)
        })

    print('kafka launch config')


def create_zk_user_data(eni_id, ipaddr):
    hostaddr = "ip-" + ipaddr.replace(".", "-")
    return """#!/bin/bash
sudo -s 
apt-get update
apt-get install -y openjdk-8-jdk zookeeperd python2.7 python-pip jq
pip install awscli
aws configure set region eu-central-1
aws configure set aws_access_key_id """ + ACCESS_KEY_ID + """
aws configure set aws_secret_access_key """ + ACCESS_SECRET_KEY + """
export INSTANCEID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 attach-network-interface --region eu-central-1 --instance-id $INSTANCEID --device-index 1 --network-interface-id """ + eni_id + """
echo "127.0.0.1 """ + hostaddr + """" >> /etc/hosts
echo "127.0.0.1" $HOSTNAME >> /etc/hosts
sed -i "/#server.1=zookeeper1:2888:3888/c\server.1=10.0.0.11:2888:3888" /etc/zookeeper/conf/zoo.cfg
sed -i "/#server.2=zookeeper2:2888:3888/c\server.2=10.0.1.11:2888:3888" /etc/zookeeper/conf/zoo.cfg
sed -i "/#server.3=zookeeper3:2888:3888/c\server.3=10.0.2.11:2888:3888" /etc/zookeeper/conf/zoo.cfg
sleep 15
ifconfig eth1 up
ifconfig eth1 """ + ipaddr + """ netmask 255.255.255.0
GATEWAY_IP=$( /sbin/ip route | awk '/default/ { print $3 }' )
echo -e "auto eth1\niface eth1 inet dhcp\n  post-up ip route add default via $GATEWAY_IP dev eth1 tab 2\n  post-up ip rule add from """ + ipaddr + """/32 tab 2 priority 600" > /etc/network/interfaces.d/eth1.cfg
service networking restart
sleep 10
LOCALIP=$(ifconfig eth1 | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1)
LOCALID=$(echo "$LOCALIP" | cut -c6 )
echo $((LOCALID + 1)) > /etc/zookeeper/conf/myid
service zookeeper stop
service zookeeper start
"""


def create_kafka_user_data(alloc_id):
    return """#!/bin/bash
sleep 60
sudo -s 
echo "127.0.1.1 ip-10-0-1-21" >> /etc/hosts
apt-get update
apt-get install -y openjdk-8-jdk python2.7 python-pip
pip install awscli
aws configure set region eu-central-1
aws configure set aws_access_key_id """ + ACCESS_KEY_ID + """
aws configure set aws_secret_access_key """ + ACCESS_SECRET_KEY + """
LOCALIP=$(ifconfig ens3 | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1)
query=$(aws ec2 describe-addresses --allocation-ids """ + alloc_id + """)
query=$(echo "$query" | jq ".Addresses[0]")
INSTANCEID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
if echo "$query" | jq -e 'has("AssociationId")' > /dev/null; then echo "address already associated"; else aws ec2 associate-address --allocation-id \"""" + alloc_id + """\" --instance-id "$INSTANCEID" --no-allow-reassociation --private-ip-address "$LOCALIP" --region eu-central-1; fi
sleep 15
wget http://it.apache.contactlab.it/kafka/2.3.0/kafka_2.12-2.3.0.tgz
mkdir /opt/kafka
tar xvzf kafka_2.12-2.3.0.tgz -C /opt/kafka
rm kafka_2.12-2.3.0.tgz
cd /opt/kafka/kafka_2.12-2.3.0/config
sed -i "/zookeeper.connect=localhost:2181/c\zookeeper.connect=10.0.0.11:2181,10.0.1.11:2181,10.0.2.11:2181" server.properties
MYIP=$(dig +short myip.opendns.com @resolver1.opendns.com)
LOCALID=${LOCALIP:5}
LOCALID=$(echo $LOCALID | tr -d .)
sed -i "/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/c\\advertised.listeners=PLAINTEXT:\/\/$MYIP:9092" server.properties
sed -i "/#listeners=PLAINTEXT:\/\/:9092/c\listeners=PLAINTEXT:\/\/0.0.0.0:9092" server.properties
sed -i "/num.partitions=1/c\\num.partitions=2" server.properties
sed -i "/broker.id=0/c\\broker.id=$LOCALID" server.properties
sed -i "/log.retention.hours=168/c\log.retention.hours=2" server.properties
echo -e "\ndefault.replication.factor=2" >> server.properties
echo -e "\nreserved.broker.max.id=10000" >> server.properties
echo "127.0.0.1" $HOSTNAME >> /etc/hosts
#mount volume
#mkfs.ext4 /dev/xvdb
#mkdir /broker
#mount /dev/xvdb /broker
#rm -rf /broker
#sed -i "/log.dirs=/tmp/kafka-logs/c\log.dirs=/broker" server.properties
../bin/kafka-server-start.sh ../config/server.properties
"""
