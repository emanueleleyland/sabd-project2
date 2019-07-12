import dns


def create_redis_configuration(client, subnet_group_name, route53_client, sg):
    cache_cluster_id = 'redis-cluster'
    cluster = client.create_cache_cluster(
        CacheClusterId=cache_cluster_id,
        AZMode='single-az',
        NumCacheNodes=1,
        CacheNodeType='cache.m4.large',
        Engine='redis',
        EngineVersion='3.2.10',
        CacheSubnetGroupName=subnet_group_name,
        SecurityGroupIds=[
            sg.id
        ],
        Port=6379,
    )
    print(cluster)
    address = input("Enter redis endpoint: ")
    dns.create_elasticache_dns_record(route53_client, address)


def create_subnet_group(client, subnets):
    return client.create_cache_subnet_group(
        CacheSubnetGroupName="cache-subnet-group",
        CacheSubnetGroupDescription="cache_subnet_group",
        SubnetIds=[
            subnets[0].id,
            subnets[1].id,
            subnets[2].id
        ]
    )