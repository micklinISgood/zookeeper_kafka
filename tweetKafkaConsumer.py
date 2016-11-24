from kafka import KafkaConsumer
import boto.sqs,boto.sns,json, inspect, threading, logging, time, requests
lock = threading.Lock()
sns_conn = boto.sns.connect_to_region("us-west-2")


KEY = "UpdateKeyWords" 


from alchemyapi import AlchemyAPI
alchemyapi = AlchemyAPI()


# logging.basicConfig(level=logging.DEBUG,
#                     format='(%(threadName)-10s) %(message)s',)

consumer = KafkaConsumer('tweet',bootstrap_servers='localhost:9092')

def worker():

	while True:

	    ret =[]

	    lock.acquire()
	    # for message in consumer:
	    partition = consumer.poll(1)
	    # print [name for name,thing in inspect.getmembers(message)]
	    if partition:
	    	# print [name for name,thing in inspect.getmembers(message)]
	    	for p in partition:
	    		# print [name for name,thing in inspect.getmembers(p)]
	    		for d in partition[p]:
	    			# print d.value 
	    			ret.append(json.loads(d.value))


	 
	    lock.release()

	    if ret:
	    	for d in ret:
	    		send_to_sns(d)


def send_to_sns(ret):

		for k in ret:
			# print k["status"]
			response = alchemyapi.sentiment("text", k["status"])
			# print response["language"]
			# print response["docSentiment"]
			if "docSentiment" not in response:
				k["sentiment"]=20
				continue
				
			if "score" not in response["docSentiment"]:
				k["sentiment"]=0
			else:
				k["sentiment"]=int(float(response["docSentiment"]["score"])*10)



		res={}
		res["action"] = KEY
		res["data"] = ret
		sns_conn.publish(
					topic="arn:aws:sns:us-west-2:631081141903:sns-http",
					message=json.dumps(res)
		)



		'''
		do analysis with pending & ret[i]["sentiment"] = analyzed score
		socket.send(json.dump(ret)) 
		'''



threads = []
for i in range(2):
    t = threading.Thread(target=worker, name="worker"+str(i))
    t.setDaemon(True)
    threads.append(t)
    t.start()

#let worker threads have time to work, main thread just sleeps
while True:
	time.sleep(20)
