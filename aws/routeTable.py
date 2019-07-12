def createRouteTable(vpc, ig):
    route_table = vpc.create_route_table()
    route = route_table.create_route(
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=ig.id
    )

    print(route_table.id)
    return route_table