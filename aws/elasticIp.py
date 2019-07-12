def createIps(eips, ec2_client, resources):
    resources['eip'] = []
    for i in range(3):
        eips.append(ec2_client.allocate_address(
            Domain='vpc'
        ))
        resources['eip'].append({
            'id': eips[i]
        })
    print(eips)
