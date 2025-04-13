import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# SDK Client
client = greengrasssdk.client("iot-data")

def lambda_handler(event, context):
    # TODO1: Get your data
    # TODO2: Calculate max CO2 emission
    maxCounter = 0.0
    for record in event:
        CO2_val = float(record['vehicle_CO2'])
        vehicle_stat = record['vehicle_id'] 

        if CO2_val > maxCounter:
            maxCounter = CO2_val
        
    # TODO3: Return the result
    client.publish(
        topic="iot/Vehicle_" + vehicle_stat,
        queueFullPolicy="AllOrException",
        payload=json.dumps({"max_CO2": maxCounter, }),
    )

    return
