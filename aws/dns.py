HOSTEDZONEID = "ZD1F6JU85TMA3"


def create_kafka_dns_record(ipaddress, route53):
    print(ipaddress)
    kafka_dns_record = route53.change_resource_record_sets(
        HostedZoneId=HOSTEDZONEID,
        ChangeBatch={
            'Changes': [
                {
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': 'kafka.cini-project.cloud',
                        'Type': 'A',
                        'TTL': 86400,
                        'ResourceRecords': [
                            {
                                'Value': ipaddress[0]['PublicIp']
                            },
                            {
                                'Value': ipaddress[1]['PublicIp']
                            },
                            {
                                'Value': ipaddress[2]['PublicIp']
                            },
                        ],
                        # 'AliasTarget': {
                        #     'HostedZoneId': HOSTEDZONEID,
                        #     'DNSName': 'ns-384.awsdns-48.com',
                        #     'EvaluateTargetHealth': False,
                        # },
                    },
                },
            ]
        },
    )

    print(kafka_dns_record)


def create_elasticache_dns_record(route53, ip):
    return route53.change_resource_record_sets(
        HostedZoneId=HOSTEDZONEID,
        ChangeBatch={
            'Changes': [
                {
                    'Action': 'UPSERT',
                    'ResourceRecordSet': {
                        'Name': 'redis.cini-project.cloud',
                        'Type': 'CNAME',
                        'TTL': 86400,
                        'ResourceRecords': [
                            {
                                'Value': ip
                            },
                        ],
                    },
                },
            ]
        },
    )
