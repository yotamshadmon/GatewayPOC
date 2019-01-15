import httplib
import uuid
import random
import time
import json
import argparse
import sys
from kazoo.client import KazooClient, KazooState
import EnginesHashRing
import logging

NUMBER_OF_USERS = 10000 

#logging.basicConfig(level=logging.ERROR)
logging.basicConfig(filename='netproc.log', level=logging.ERROR)
logger = logging.getLogger('NetProcSimulator')
logger.setLevel(logging.INFO)

class NetProcSimulator:
    def __init__(self, zookeeperAddr='127.0.0.1:2181'):
        self._userSequence={}
        self._readyEngines=[]

        # register to zookeeper
        zk = KazooClient(hosts=zookeeperAddr)
        # set connection state listener
        def my_listener(state):
            if state == KazooState.LOST:
                logger.error("ZK connection LOST")
            elif state == KazooState.SUSPENDED:
                logger.error("ZK connection SUSPENDED")
            else:
                logger.info("ZK connection CONNECTED")
        zk.add_listener(my_listener)
        zk.start()
        
        # init engines hash ring
        self._enginesHashRing=EnginesHashRing.EnginesHashRing([])

        # set watcher for new engines
        @zk.ChildrenWatch("/Engines/Registered")
        def watch_registered(regEngines):
            logger.info("Registered engines are now: %s" % regEngines)
            engines=[]
            for engineName in regEngines:
                data, stat = zk.get("/Engines/Registered/%s"%engineName)
                logger.info("Name: %s Registered, data: %s" % (engineName, data.decode("utf-8")))
                engines.append(data)
            self._enginesHashRing=EnginesHashRing.EnginesHashRing(engines)

        # set watcher for new engines
        @zk.ChildrenWatch("/Engines/Ready")
        def watch_ready(readyEngines):
            self._readyEngines=[]
            logger.info("Ready engines are now: %s" % readyEngines)
            for engineName in readyEngines:
                data, stat = zk.get("/Engines/Ready/%s"%engineName)
                logger.info("Name: %s Ready, data: %s" % (engineName, data.decode("utf-8")))
                self._readyEngines.append(data)

    def sendActivity(self, engineAddress, reqBody):
        ret=False
        if engineAddress != None and engineAddress in self._readyEngines:
            try:
                conn = httplib.HTTPConnection(engineAddress)
                
                #  send activity to engine
                conn.request('POST', '/', 
                             body=reqBody,
                             headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"})
                response = conn.getresponse()
                ret=True
#             logger.debug(response.reason)
            except httplib.HTTPException as httpExcept:
                logger.error(httpExcept)
            except httplib.NotConnected as httpExcept:
                logger.error(httpExcept)
            except:
                logger.error('HTTP exceptiion')
        return ret
            
    def startSend(self):
        while True:
            if len(self._readyEngines) > 0:
                # generate activity
                activity=self._randomActivity()
                activityStr=json.dumps(activity)
                reqBody=('activity=%s' % activityStr)
                # get engine address
                engineAddress=self._enginesHashRing.user2engine(activity['user'])
                # send activity request to engine
                if self.sendActivity(engineAddress, reqBody) == False:
                    logger.warn('Failed to send activity to engine: %s' % engineAddress)
                    engineAddress=self._enginesHashRing.user2backupEngine(activity['user'])
                    logger.warn('Sending to backup engine: %s' % engineAddress)
                    if self.sendActivity(engineAddress, reqBody) == False:
                        logger.error('Failed to send activity to BACKUP engine: %s' % engineAddress)
                        self._userSequence[activity['user']]=self._userSequence[activity['user']] - 1
                        time.sleep(0.5)
                time.sleep(0.05)

    def _randomActivity(self):
            tanents=['ForcePoint', 'SkyFence', 'Melanox', 'LivePerson', 'NICE', 'SAP']
            actions=['login', 'upload', 'share', 'download']
            newActivity={}
            newActivity['ID']=uuid.uuid4().get_node()
            newActivity['tenant']=random.choice(tanents)
            user=('user%d' % (random.randint(0, NUMBER_OF_USERS)))
            newActivity['user']=user
            if user in self._userSequence:
                self._userSequence[user]=self._userSequence[user] + 1
            else:
                self._userSequence[user]=1
            newActivity['userSequence']=self._userSequence[user]
            newActivity['action']=random.choice(actions)
            newActivity['ip']=("%d.%d.%d.%d" % (random.randint(1,255), random.randint(0,255), random.randint(0,255), random.randint(0,255)))
            newActivity['timestamp']=int(round(time.time() * 1000))
            newActivity['payload']=('p'*2000)
            return newActivity
        
if __name__ == "__main__":
    argParser = argparse.ArgumentParser(description='NetProc Simulator')
    argParser.add_argument('-z', '--zookeeper', help='zookeeper', default='127.0.0.1:2181')    
    
    args = argParser.parse_args(sys.argv[1:])

    nps = NetProcSimulator(args.zookeeper)
    nps.startSend()
