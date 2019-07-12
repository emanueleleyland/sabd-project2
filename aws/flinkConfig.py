import boto3
from config import RELEASE_LABEL_EMR, INSTANCE_TYPE_LARGE_MEM, KEY_PEM

NUMWORKERS = 2

def launch_flink_cluster(subnets, vpc):
    emr_client = boto3.client('emr')
    ec2 = boto3.resource('ec2')
    master_sg = ec2.create_security_group(
        GroupName='master', Description='master sec group', VpcId=vpc.id)
    master_sg.authorize_ingress(
        IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
    print(master_sg.id)

    slave_sg = ec2.create_security_group(
        GroupName='slave', Description='slave sec group', VpcId=vpc.id)
    slave_sg.authorize_ingress(
        IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                       {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
    print(slave_sg.id)

    cluster = emr_client.run_job_flow(
        Name='SABD-Cluster',
        LogUri='s3://sabd-emr-log/elasticmapreduce/',
        ReleaseLabel="emr-5.24.0",
        Instances={
            'EmrManagedMasterSecurityGroup': master_sg.id,
            'EmrManagedSlaveSecurityGroup': slave_sg.id,
            'Ec2KeyName': KEY_PEM,
            'InstanceFleets': [
                {
                    'Name': 'master',
                    'InstanceFleetType': 'MASTER',
                    'TargetOnDemandCapacity': 1,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': 'm4.xlarge',
                            'WeightedCapacity': 1
                        }
                    ]
                },
                {
                    'Name': 'slave',
                    'InstanceFleetType': 'CORE',
                    'TargetOnDemandCapacity': NUMWORKERS,
                    'InstanceTypeConfigs': [
                        {
                            'InstanceType': 'm4.xlarge',
                            'WeightedCapacity': 1
                        }
                    ]
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2SubnetIds': [
                subnets[0].id,
                subnets[1].id,
                subnets[2].id
            ],

        },
        Applications=[
            {
                'Name': 'Flink',
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',

    )
    job_flow_id = cluster['JobFlowId']
    print(job_flow_id)
