#!/usr/bin/env python3
import json
import requests
from datetime import timedelta, datetime, tzinfo
import time
import calendar

#
# Configuration values
#

# Location of CoScale install
ENV = "https://app.coscale.com/api/v1/app"

# Name of the metric
METRIC_NAME = "Docker container total cpu usage in cores"

# Application data
APP_ID = "000000-08e8-4f72-0000-220ff6600000"
APP_TOKEN = "000000-ebd1-4880-0000-3f4c06500000"

# Data to retrieve
START_DATE = datetime(2018, 5, 25)
END_DATE = datetime(2018, 5, 27)

# Images to retrieve data for, these should match the ones in CoScale Docker group
IMAGES = [
    "docker.io/kidk/training-images-calc_words",
    "docker.io/kidk/training-images-web",
]

#
# DO NOT CHANGE ANYTHING BELOW THIS LINE
#
def dt2ts(dt):
    """Converts a datetime object to UTC timestamp"""
    return calendar.timegm(dt.utctimetuple())

def debug(variable, fail=True):
    print(json.dumps(variable, indent=2, sort_keys=True))
    if fail:
        exit(0)

print("Retrieving data timerange from %s to %s" % (
    START_DATE, END_DATE
))
START_TIME = dt2ts(START_DATE)
END_TIME = dt2ts(END_DATE)

class API:

    token = None
    app = None
    env = None

    def __init__(self, env, app, token):
        r = requests.post(
            "{env}/{app}/login/".format(env=env, app=app), data={"accessToken": token}
        )
        self.token = json.loads(r.text)["token"]
        self.app = app
        self.env = env

    def do_post():
        return None

    def do_get():
        return None

    def get_metric_id(self, name):
        r = requests.get(
            "{env}/{app}/metrics/?selectByName={name}".format(
                env=self.env, app=self.app, name=name
            ),
            headers={"HTTPAuthorization": self.token},
            data={},
        )
        metrics = json.loads(r.text)

        return metrics[0]["id"]

    def get_servergroup_children(self, id, start, stop):
        # Retrieve group children
        r = requests.get(
            "{env}/{app}/servergroups/{group_id}/servergroups/?start={start}&stop={stop}".format(
                env=self.env, app=self.app, group_id=id, start=start, stop=stop
            ),
            headers={"HTTPAuthorization": self.token},
        )

        return json.loads(r.text)

    def get_servergroup_servers(self, id, start, stop):
        r = requests.get(
            "{env}/{app}/servergroups/{group_id}/servers/?start={start}&stop={stop}".format(
                env=self.env, app=self.app, group_id=id, start=start, stop=stop
            ),
            headers={"HTTPAuthorization": self.token},
        )

        return json.loads(r.text)

    def get_servergroup(self, name):
        # Retrieve group ID
        r = requests.get(
            "{env}/{app}/servergroups/?selectByName={name}".format(
                env=self.env, app=self.app, name=name
            ),
            headers={"HTTPAuthorization": self.token},
            data={},
        )

        return json.loads(r.text)[0]

    def get_data(self, metric, servers, start, stop):
        postdata = {
            "start": range_start_time,
            "stop": range_stop_time,
            "ids": [
                {
                    "metricId": METRIC_ID,
                    "dimensionsSpecs": [],
                    "subjects": "%s" % servers,
                    "viewtype": "AVG",
                }
            ],
        }
        r = requests.post(
            "{env}/{app}/data/dimension/getCalculated/calculated/?function=summary&mode=average".format(
                env=self.env, app=self.app
            ),
            headers={"HTTPAuthorization": self.token},
            data={"data": json.dumps(postdata)},
        )

        return json.loads(r.text)

    def get_all_servers_in_group_helper(self, group):
        serverIds = group['serverIds']

        for child_group in group['servergroups']:
            serverIds = serverIds + self.get_all_servers_in_group_helper(child_group)

        return serverIds

    def get_all_servers_in_group(self, id, start, stop):
        # Retrieve all containers
        r = requests.get(
            "{env}/{app}/servergroups/{group_id}/?expand=servergroups&expand=serverIds&start={start}&stop={stop}".format(
                env=self.env, app=self.app, group_id=id, start=start, stop=stop
            ),
            headers={"HTTPAuthorization": self.token},
        )
        groups = json.loads(r.text)

        # Get list of all container id's
        serverIds = self.get_all_servers_in_group_helper(groups)

        return list(set(serverIds))  # Remove duplicates


# Init API and login
API = API(ENV, SOURCE['id'], SOURCE['token'])

# Retrieve the metric ID for metric name
METRIC_ID = API.get_metric_id(METRIC_NAME)
print("Retrieved metricId %s for '%s'" % (METRIC_ID, METRIC_NAME))

# Retrieve Docker group children
image_group = API.get_servergroup('Docker')
images_group = API.get_servergroup_children(image_group['id'], start=START_TIME, stop=END_TIME)

# Prepare list of all containers running for image
containers = {}
for group in images_group:
    image_name = group["name"]
    if image_name in IMAGES:
        # Add element to containers list
        if image_name not in containers:
            containers[image_name] = {}

        # Retrieve subgroups
        subgroups = API.get_servergroup_children(group["id"], start=START_TIME, stop=END_TIME)

        # Retrieve containers from subgroups
        for subgroup in subgroups:
            servers = API.get_servergroup_servers(subgroup['id'], START_TIME, END_TIME)

            # Add servers to containers list
            for server in servers:
                server_id = server['id']
                if server_id not in containers:
                    containers[image_name][server_id] = server['name']

print("Found following containers for selected images")
#debug(containers, False)

# Retrieve for each image type and containers
metric_data = {}
datapoints = 0
for image in containers:
    # Check if metric_data image exists
    if image not in metric_data:
        metric_data[image] = {}

    for container_id in containers[image]:
        metric_data[image][container_id] = {}

    for n in range(int ((END_DATE - START_DATE).days)):
        range_start_time = dt2ts(START_DATE + timedelta(n))
        range_stop_time = dt2ts(START_DATE + timedelta(n + 1))

        # Retrieve data for all containers for this timerange
        servers = ','.join("s%s" % str(server_id) for server_id in containers[image])
        values = API.get_data(METRIC_ID, servers, range_start_time, range_stop_time)

        # Parse data for each container and add it
        for value in values:
            container_id = int(value['s'][2:-1]) # Remove the [s at beginning and ] at the end
            metric_data[image][container_id][range_start_time] = {
                'start': range_start_time,
                'stop': range_stop_time,
                'value': value['calc'][0],
            }
            datapoints = datapoints + 1

print("Fetched %s datapoints" % datapoints)
#debug(metric_data, False)

# Retrieve all namespaces
namespaces = {}

# Retrieve Namespace group children
namespace_group = API.get_servergroup('Namespaces')
namespaces_group = API.get_servergroup_children(namespace_group['id'], start=START_TIME, stop=END_TIME)


# Retrieve servers in namespaces
for namespace in namespaces_group:
    # Prepare data
    namespace_name = namespace['name']
    namespace_id = namespace['id']
    if namespace_name not in namespaces:
        namespaces[namespace_name] = {}

    # Loop days to limit amount of servers we get back
    for n in range(int ((END_DATE - START_DATE).days)):
        range_start_time = dt2ts(START_DATE + timedelta(n))
        range_stop_time = dt2ts(START_DATE + timedelta(n + 1))

        serverIds = API.get_all_servers_in_group(namespace_id, range_start_time, range_stop_time)

        # Check images
        for image in metric_data:
            if image not in namespaces[namespace_name]:
                namespaces[namespace_name][image] = {}

            for container in metric_data[image]:
                total = 0.0
                if container in serverIds:
                    value = metric_data[image][container][range_start_time]['value']
                    if value is not 'null' and value is not None:
                        total += value

                date = datetime.fromtimestamp(range_start_time).strftime('%Y-%m-%d')
                if date not in namespaces[namespace_name][image]:
                    namespaces[namespace_name][image][date] = 0

                namespaces[namespace_name][image][date] += total

debug(namespaces)
