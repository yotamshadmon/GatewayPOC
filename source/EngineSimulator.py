import collections
import json
import sys
import argparse
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urlparse
import cgi
from kazoo.client import KazooClient
import logging
import socket
import uuid
import EnginesHashRing
import random
import time
from cgi import log
import Statistics

#logging.basicConfig(level=logging.ERROR)
logging.basicConfig(filename='engine.log', level=logging.ERROR)
logger = logging.getLogger('EngineSimulator')
logger.setLevel(logging.ERROR)

class PersistentDict(collections.MutableMapping):
    def __init__(self, fileName):
        self.store = dict()
        self._fileName=fileName
#         self._readFromfile()
        
    def __del__(self):
        pass
#         self._writeTofile()
        
    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value
        

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key
    
    def _readFromfile(self):
        try:
            with open(self._fileName, 'r') as fp:
                self.store = json.load(fp)
        except:
            logger.debug('File not found %s' % self._fileName)
     
    def _writeTofile(self):
        with open(self._fileName, 'w') as fp:
            json.dump(self.store, fp)    

class EngineSimulator:
    def __init__(self, port=8000, zookeeperAddr='127.0.0.1:2181', kafkaAddr='localhost:9092', hostIP=''):
        self._cadence=Statistics.Cadence('WRITE Activities per second')
        self._readCadence=Statistics.Cadence('READ Activities per second')
        self._workTopics=[]
        self._allTopics=[]
        if hostIP == '':
            self._ipAddress=("%s:%d"%(socket.gethostbyname(socket.gethostname()), port))
        else:
            self._ipAddress=("%s:%d"%(hostIP, port))
            
        self._nodeName=("engine_%s" % self._ipAddress)
        self._progressVent='/'

        # cache DBs
        self._localDbs={}
        # consumer
        self._consumer = KafkaConsumer(bootstrap_servers=kafkaAddr,
                                       auto_offset_reset='earliest',
                                       consumer_timeout_ms=502)
        # producer
        self._producer = KafkaProducer(bootstrap_servers=kafkaAddr)
        # zookeeper client
        self._zk = KazooClient(hosts=zookeeperAddr)
        self._zk.start()
        
        # init engines hash ring
        self._enginesHashRing=EnginesHashRing.EnginesHashRing([])
    
    def __del__(self):
        self._producer.close()
        self._consumer.close()
    
    def zkRegister(self):
        self._zk.ensure_path("/Engines/Registered")
        self._zk.create(("/Engines/Registered/%s" % self._nodeName) , value=self._ipAddress, ephemeral=True)
        
        logMsg={}
        logMsg['timestamp']=int(time.time()*1000)
        logMsg['text']="Engine registered to ZK"
        logMsg['engineIP']=self._ipAddress
        self._producer.send('logging', json.dumps(logMsg))
        
#         zk.delete(("/Engines/engine_%d" % self._ipAddress))
#         if zk.exists("/Engines"):
#             data, stat = zk.get("/Engines")
#             print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
#             children = zk.get_children("/Engines")
#             print("There are %s children with names %s" % (len(children), children))
#             for znode in children:
#                 data, stat = zk.get("/Engines/%s"%znode)
#                 print("Name: %s, Version: %s, data: %s" % (znode, stat.version, data.decode("utf-8")))

    def zkWatch(self):
        # set watcher for new engines
        @self._zk.ChildrenWatch("/Engines/Registered")
        def watch_children(children):
            logger.debug("Engines are now: %s" % children)
            engines=[]
            for engineName in children:
                data, stat = self._zk.get("/Engines/Registered/%s"%engineName)
                logger.debug("Name: %s, Version: %s, data: %s" % (engineName, stat.version, data.decode("utf-8")))
                engines.append(data)
            self._enginesHashRing=EnginesHashRing.EnginesHashRing(engines)
            
            # update work topics
            self._workTopics=self._enginesHashRing.workTopics2engine(self._ipAddress)
            backupTopics=self._enginesHashRing.backupTopics2engine(self._ipAddress)

            logger.info('backup topics [%d]:' % len(backupTopics))
            logger.info(backupTopics)
            logger.info('work topics [%d]:' % len(self._workTopics))
            logger.info(self._workTopics)

#             # subscribe to new kafka topics
#             allTopics=self._workTopics + backupTopics
#             if len(allTopics) > 0:
#                 self._consumer.unsubscribe()
#                 self._consumer.subscribe(allTopics)

            # assign to new kafka topics
            self._allTopics=self._workTopics + backupTopics
            tps=[]
            for topic in self._allTopics:
                tp = TopicPartition(topic, 0)
                tps.append(tp)   
            self._consumer.assign(tps)

            for topic in self._allTopics:
                if topic not in self._localDbs:
                    self._localDbs[topic]=PersistentDict("%s_%s.json" % (self._ipAddress, topic))
                    
            logMsg={}
            logMsg['timestamp']=int(time.time()*1000)
            logMsg['text']="Engine ZK change notification"
            logMsg['engineIP']=self._ipAddress
            logMsg['workTopics']=self._workTopics
            logMsg['backupTopics']=backupTopics
            self._producer.send('logging', json.dumps(logMsg))

    def _checkLoadTopicsProgress(self, topicProgress, msg):
        highwater=self._consumer.highwater(TopicPartition(msg.topic, msg.partition))
        topicProgress[msg.topic]['offset']=msg.offset
        topicProgress[msg.topic]['highwater']=highwater
        totalOffset=0
        totalHighwater=0
        totalTopicsUpdated=0
        for topic in topicProgress:
            totalOffset = totalOffset + topicProgress[topic]['offset']
            totalHighwater = totalHighwater + topicProgress[topic]['highwater']
            if topicProgress[topic]['highwater']:
                totalTopicsUpdated = totalTopicsUpdated + 1
                
        totalProgress=0
        if totalHighwater > 0:
            totalProgress = float(totalOffset) / float(totalHighwater)
            
        topicsProgress = float(totalTopicsUpdated) / float(len(self._allTopics))

        logger.debug(('READ:    %s: %s' % (msg.topic, msg.value)))

        return totalProgress, topicsProgress
        
        
    def loadTopics(self):
        logger.info('loadTopics() started %s' % time.ctime(time.time()))
        topicProgress={}
        for topic in self._allTopics:
            topicProgress[topic]={'offset':0, 'highwater':0}
        
        # initiate load cadence statistics object
        startupCadence = Statistics.Cadence('Startup cadence')
            
        # build local DB from topics
        if len(self._allTopics) > 0:
            msgCount=0
            for msg in self._consumer:
                act = json.loads(msg.value)
                self._localDbs[msg.topic][act['user']]=msg.value
                
                # check progress
                startupCadence.inc()
                msgCount = msgCount + 1
                if logger.getEffectiveLevel() <= logging.INFO and msgCount % 1000 == 0:
                    totalProgress, topicsProgress = self._checkLoadTopicsProgress(topicProgress, msg)
                    logger.info("Progress: %d%%, topics loaded: %d%%" % ((totalProgress*100), (topicsProgress*100)))
                    
                    logMsg={}
                    logMsg['timestamp']=int(time.time()*1000)
                    logMsg['text']="Engine startup progress"
                    logMsg['engineIP']=self._ipAddress
                    logMsg['topics']=(topicsProgress*100)
                    logMsg['total']=(totalProgress*100)
                    self._producer.send('logging', json.dumps(logMsg))

                    if topicProgress > 0.9 and totalProgress > 0.9:
                        break

        self._logStatistics(startupCadence)
        logger.info('loadTopics() ended   %s' % time.ctime(time.time()))

    def zkReady(self):
        self._zk.ensure_path("/Engines/Ready")
        self._zk.create(("/Engines/Ready/%s" % self._nodeName) , value=self._ipAddress, ephemeral=True)

        logMsg={}
        logMsg['timestamp']=int(time.time()*1000)
        logMsg['text']="Engine ready to ZK"
        logMsg['engineIP']=self._ipAddress
        self._producer.send('logging', json.dumps(logMsg))

    def _checkActivityUserSeq(self, newActivity, topic):
        pervSeq=0
        ret=False
        if len(self._localDbs[topic]) > 0:
            if newActivity['user'] in self._localDbs[topic]:
                rec=self._localDbs[topic][newActivity['user']]
                dictRec=json.loads(rec)
                pervSeq = dictRec['userSequence']
        newSeq = newActivity['userSequence']
        if newSeq == pervSeq + 1:
            logger.debug('Activity userSequence MATCH %d' % (newSeq))
            ret=True
        elif pervSeq-(newSeq-1) < 0:
            logger.warn('Activity userSequence ERROR %d. prev seq:%d last seq: %d' % (pervSeq-(newSeq-1), pervSeq, newSeq))
            ret=False
        elif pervSeq-(newSeq-1) > 0:
            logger.debug('Activity userSequence ERROR %d. prev seq:%d last seq: %d' % (pervSeq-(newSeq-1), pervSeq, newSeq))
            ret=False
            
        if ret==False and logger.getEffectiveLevel() == logging.DEBUG:
            logMsg={}
            logMsg['timestamp']=int(time.time()*1000)
            logMsg['text']="Activity userSequence ERROR"
            logMsg['engineIP']=self._ipAddress
            logMsg['difference']=pervSeq-(newSeq-1)
            logMsg['topic']=topic
            logMsg['user']=newActivity['user']
            logMsg['userSequence']=newSeq
            self._producer.send('logging', json.dumps(logMsg))
            
        return ret
        
    def ProcessActivity(self, activity):
            topicType='N'
            topic=self._enginesHashRing.user2topic(activity['user'])
            if topic in self._workTopics:
                logger.debug('Received activity from WORKTOPIC topic %s' % topic)
                topicType='W'
            elif topic in self._allTopics:
                logger.info('Received activity from BACKUP topic %s' % topic)
                topicType='B'
            else:
                logger.error('Received activity for topic not cached (not WORKING topic and not BACKUP topic). topic %s' % topic)

#             logMsg={}
#             logMsg['timestamp']=int(time.time()*1000)
#             logMsg['text']="User topic"
#             logMsg['engineIP']=self._ipAddress
#             logMsg['topic']=topic
#             logMsg['topicType']=topicType
#             self._producer.send('logging', json.dumps(logMsg))
            self._logStatistics(self._cadence, 1000)

            self._checkActivityUserSeq(activity, topic)
            self._updateCache()
#             self._persistCache()

            jsonActivity=json.dumps(activity)
            # add activity to local cache
            self._localDbs[topic][activity['user']]=jsonActivity
            self._producer.send(topic, jsonActivity)
            logger.debug('WRITE:    %s: %s' % (topic, activity))
            self._activityProgress()
#             time.sleep(0.050)
            
    def _activityProgress(self):
        sys.stdout.write('\b')
        if self._progressVent=='/':
            self._progressVent='-'
        elif self._progressVent=='-':
            self._progressVent='\\'
        elif self._progressVent=='\\':
            self._progressVent='|'
        elif self._progressVent=='|':
            self._progressVent='/'
        sys.stdout.write(self._progressVent)
        sys.stdout.flush()
            
        
    def _persistCache(self):
        if random.randint(0, 10) == 1:
            for db in self._localDbs:
                self._localDbs[db]._writeTofile()
        
    def _updateCache(self):
        pollList = self._consumer.poll(timeout_ms=0, max_records=len(self._workTopics)*100)
        for tp in pollList:
            for msg in pollList[tp]:
                dictActivity=json.loads(msg.value)
                if tp.topic in self._localDbs:
                    db=self._localDbs[tp.topic]
                    user=dictActivity['user']
                    if user in db:
                        prevSeq=json.loads(db[user])['userSequence']
                        if prevSeq < dictActivity['userSequence']:
                            self._localDbs[tp.topic][dictActivity['user']] = msg.value
                logger.debug(('READ:    %s: %s' % (tp.topic, msg.value)))
                self._logStatistics(self._readCadence, 1000)
                
    def _logStatistics(self, cadence, logEvery = 0):
        if logEvery == 0 or (cadence.inc() % logEvery) == 0:
            sec, count, cad = cadence.getLastCadence()
            logMsg={}
            logMsg['timestamp']=int(time.time()*1000)
            logMsg['text']=cadence.text()
            logMsg['engineIP']=self._ipAddress
            logMsg['seconds']=sec
            logMsg['count']=count
            logMsg['cadence']=cad
            self._producer.send('logging', json.dumps(logMsg))
        

    def _checkPercentLoaded(self):
        totalHighwater=0
        totalPosition=0
        assignment=self._consumer.assignment()
        for tp in assignment:
            try:
                totalHighwater = totalHighwater + self._consumer.highwater(tp)
                totalPosition= totalPosition + self._consumer.position(tp)
            except:
                pass
        if totalHighwater == 0:
            return 100
        return totalPosition/totalHighwater * 100

def MakeHandlerClassWithParams(engineSimulator):
    class EngineHttpReqHandler(BaseHTTPRequestHandler, object):
        def __init__(self, *args, **kwargs):
            self.timeout=20
            self._engineSimulator=engineSimulator
            super(EngineHttpReqHandler, self).__init__(*args, **kwargs)
        def _set_headers(self):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
    
        def do_GET(self):
            self._set_headers()
            parsed_path = urlparse.urlparse(self.path)
            request_id = parsed_path.path
            response = 'GET response\n'
            self.wfile.write(json.dumps(response))
    
        def do_POST(self):
            self._set_headers()
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                postvars = cgi.parse_multipart(self.rfile, pdict)
            elif ctype == 'application/x-www-form-urlencoded':
                length = int(self.headers.getheader('content-length'))
                postvars = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
                #activity=json.loads('{"a":"aa", "b":"bb"}')
                activityStr=postvars['activity'][0]
                activityDict=json.loads(activityStr)
                self._engineSimulator.ProcessActivity(activityDict)
            else:
                postvars = {}
            response = 'POST response'
#             self.wfile.write(json.dumps(response))
    
        def do_HEAD(self):
            self._set_headers()
        def log_message(self, format, *args):
            return
        
    return EngineHttpReqHandler

def run(engineSimulator, server_class=HTTPServer, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, MakeHandlerClassWithParams(engineSimulator))
    logger.info('Starting httpd...')
    httpd.serve_forever()

if __name__ == "__main__":
    # parse arguments
    argParser = argparse.ArgumentParser(description='Engine Simulator')
    argParser.add_argument('-p', '--port', help='port', default=8000, type=int)    
    argParser.add_argument('-z', '--zookeeper', help='zookeeper', default='127.0.0.1:2181')    
    argParser.add_argument('-k', '--kafka', help='kafka', default='localhost:9092')    
    argParser.add_argument('-hip', '--host', help='host ip', default='')    
    args = argParser.parse_args(sys.argv[1:])
    
    # create engine simulator and load topics from kafka
    engineSim=EngineSimulator(args.port, args.zookeeper, args.kafka, args.host)
    engineSim.zkRegister()
    engineSim.zkWatch()
    engineSim.loadTopics()
    engineSim.zkReady()
    
    # start http server
    run(engineSimulator=engineSim, port=args.port)

    del engineSim
