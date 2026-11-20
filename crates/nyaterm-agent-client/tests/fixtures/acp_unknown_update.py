import json, sys

def send(value):
    print(json.dumps(value), flush=True)

request = json.loads(sys.stdin.readline())
send({'jsonrpc':'2.0','id':request['id'],'result':{'protocolVersion':1,'agentCapabilities':{}}})
send({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'future-session','update':{'sessionUpdate':'future_feature','data':'ignored'}}})
send({'jsonrpc':'2.0','method':'session/update','params':{'sessionId':'future-session','update':{'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':'known'}}}})
for line in sys.stdin:
    pass
