def createSubnets(ec2, subnets, vpc):
    for i in range(3):
        subnets.append(ec2.create_subnet(CidrBlock='10.0.' + str(i) + '.0/24', VpcId=vpc.id,
                                         AvailabilityZone='eu-central-1' + chr(97 + i)))
        print(subnets[i].id)
