from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time, json, pandas as pd, os

device_st = 0
device_end = 5
data_path = "vehicle{}.csv"
cert_fmt = "./certificates/car_{}/car_{}.cert.pem"
key_fmt = "./certificates/car_{}/car_{}.private.key"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        self.device_id = str(device_id)
        self.client = AWSIoTMQTTClient(self.device_id)
        self.client.configureEndpoint("a3en5sa7t8or55-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)
        self.client.onMessage = self.customOnMessage

    def customOnMessage(self, message):
        print(f"[{self.device_id}] ← {message.topic}: {message.payload}")
        try:
            payload = json.loads(message.payload)
            print(f"   ↳ max_CO2: {payload.get('max_CO2')}")
        except:
            print("   ↳ payload parse failed")

    def customSubackCallback(self, mid, data): print(f"[{self.device_id}] subscribed.")
    def customPubackCallback(self, mid): print(f"[{self.device_id}] published.")

    def publish(self):
        topic = "iot/vehicle_data"
        file = data_path.format(int(self.device_id) % 5)
        if not os.path.exists(file): print(f"[{self.device_id}] missing {file}"); return
        try:
            row = pd.read_csv(file).sample(n=1).iloc[0]
            payload = {
                "vehicle_id": self.device_id,
                "vehicle_CO2": float(row.get("vehicle_CO2", 0))
            }
            msg = json.dumps({"records": [payload]})
            print(f"[{self.device_id}] → {topic}: {msg}")
            self.client.publishAsync(topic, msg, 0, ackCallback=self.customPubackCallback)
        except Exception as e:
            print(f"[{self.device_id}] publish error: {e}")

    def subscribe(self):
        topic = f"iot/Vehicle_{self.device_id}/result"
        print(f"[{self.device_id}] subscribing → {topic}")
        self.client.subscribeAsync(topic, 0, ackCallback=self.customSubackCallback)

print("Loading CSVs...")
for i in range(5):
    try:
        file = data_path.format(i)
        pd.read_csv(file)
        print(f"✔ {file}")
    except: print(f"✘ {file}")

clients = []
for device_id in range(device_st, device_end):
    cert = cert_fmt.format(device_id + 1, device_id + 1)
    key = key_fmt.format(device_id + 1, device_id + 1)
    if not os.path.exists(cert) or not os.path.exists(key):
        print(f"✘ cert/key missing for {device_id}")
        continue
    try:
        c = MQTTClient(device_id, cert, key)
        c.client.connect()
        c.subscribe()
        clients.append(c)
        print(f"[{device_id}] connected.")
    except Exception as e:
        print(f"[{device_id}] connect error: {e}")

if not clients: print("no devices."); exit()

print("READY — type 's' to send, 'd' to disconnect")
while True:
    try:
        cmd = input("cmd: ")
        if cmd == "s":
            for c in clients: c.publish()
        elif cmd == "d":
            for c in clients: c.client.disconnect()
            print("Disconnected."); break
        else: print("Invalid. Use 's' or 'd'.")
    except Exception as e:
        print(f"Main loop error: {e}")
    time.sleep(1)
