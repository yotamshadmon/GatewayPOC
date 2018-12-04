import httplib
import uuid
import random
import time
import json
import argparse
import sys
from kazoo.client import KazooClient, KazooState
import EnginesHashRing

class NetProcSimulator:
    def __init__(self, port):
        self._port=port
        self._userSequence={}

        # register to zookeeper
        zk = KazooClient(hosts='127.0.0.1:2181')
        # set connection state listener
        def my_listener(state):
            if state == KazooState.LOST:
                print("ZK connection LOST")
            elif state == KazooState.SUSPENDED:
                print("ZK connection SUSPENDED")
            else:
                print("ZK connection CONNECTED")
        zk.add_listener(my_listener)
        zk.start()
        
        # init engines hash ring
        self._enginesHashRing=EnginesHashRing.EnginesHashRing([])

        # set watcher for new engines
        @zk.ChildrenWatch("/Engines/Ready")
        def watch_children(children):
            print("Engines are now: %s" % children)
            engines=[]
            for engineName in children:
                data, stat = zk.get("/Engines/Ready/%s"%engineName)
                print("Name: %s, Version: %s, data: %s" % (engineName, stat.version, data.decode("utf-8")))
                engines.append(data)
            self._enginesHashRing=EnginesHashRing.EnginesHashRing(engines)

    def sendActivity(self):
        # generate activity
        activity=self._randomActivity()
        activityStr=json.dumps(activity)
        reqBody=('activity=%s' % activityStr)
        
        # get engine address
        engineAddress=self._enginesHashRing.user2engine(activity['user'])
        if engineAddress != None:
            try:
                conn = httplib.HTTPConnection(engineAddress)
                
                #  send activity to engine
                conn.request('POST', '/', 
                             body=reqBody,
                             headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"})
                response = conn.getresponse()
#             print(response.reason)
            except httplib.HTTPException as httpExcept:
                print(httpExcept)
            except httplib.NotConnected as httpExcept:
                print(httpExcept)
            except:
                print('HTTP exceptiion')
            
        

    def _randomActivity(self):
            tanents=['ForcePoint', 'SkyFence', 'Melanox', 'LivePerson', 'NICE', 'SAP']
            actions=['login', 'upload', 'share', 'download']
            newActivity={}
            newActivity['ID']=uuid.uuid4().get_node()
            newActivity['tenant']=random.choice(tanents)
            user=('user%d' % (random.randint(0, 10)))
            newActivity['user']=user
            if user in self._userSequence:
                self._userSequence[user]=self._userSequence[user] + 1
            else:
                self._userSequence[user]=1
            newActivity['userSequence']=self._userSequence[user]
            newActivity['action']=random.choice(actions)
            newActivity['ip']=("%d.%d.%d.%d" % (random.randint(1,255), random.randint(0,255), random.randint(0,255), random.randint(0,255)))
            newActivity['timestamp']=int(round(time.time() * 1000))
            return newActivity
        
if __name__ == "__main__":
    argParser = argparse.ArgumentParser(description='NetProc Simulator')
    argParser.add_argument('-p', '--port', nargs='+', help='port', default=8000, type=int)
    
    args = argParser.parse_args(sys.argv[1:])

    nps = NetProcSimulator(args.port)
    while True:
        nps.sendActivity()
        time.sleep(0.5)
