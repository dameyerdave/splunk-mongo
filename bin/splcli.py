import splunklib.client as client
from pprint import pprint

service = client.connect(schema='http', host='127.0.0.1', port=8089, username='admin', password='5plunk>%Txy', app='adhfile')
configs = service.confs
for cf in configs:
    print("\n--[[%s]]--" % cf.name)
    for stanza in cf:
        print("\n[%s]" % stanza.name)
        for key, value in stanza.content.items():
            print("%s = %s" % (key, value))
