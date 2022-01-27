from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for _ in range(100):
	producer.send('Orders1', key=b'kushal-ASUS-TUF-Dash-F15-FX516PM-FX516PM_osqueryd', value=b'{"name":"pack_system-snapshot_some_query1","hostIdentifier":"784c0831-9b3f-5a44-af06-903a4d67f7ea","calendarTime":"Fri Jan  7 15:10:28 2022 UTC","unixTime":1641568228,"epoch":0,"counter":0,"numerics":false,"columns":{"arch":"amd64","maintainer":"Ubuntu Developers <ubuntu-devel-discuss@lists.ubuntu.com>","name":"gvfs-backends","priority":"optional","revision":"1ubuntu1","section":"gnome","size":"1932","source":"gvfs","status":"install ok installed","version":"1.44.1-1ubuntu1"},"action":"snapshot"}')

