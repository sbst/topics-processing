import postgresql


db = postgresql.open('')
delta = 0
# topic_id  DELTA
# 32		-0.02
#for month in range(1,13):
query = "SELECT * FROM GETDELTASECOND WHERE delta is not null ORDER BY topic;"
topicData = db.query(query)

for i in range(0,len(topicData)-1):
	if topicData[i][0] == topicData[i+1][0]:
		delta = delta + topicData[i][1] + topicData[i+1][1]
	elif (topicData[i][0] != topicData[i+1][0]) & (delta == 0):
		delta = topicData[i][1]
		#db.query("INSERT INTO deltas_neuro(topic,weigth) VALUES ('{}',{});".format(topicData[i][0],delta))
		db.query("UPDATE topics SET weight={} WHERE topic='{}'".format(delta,topicData[i][0]))
		delta = 0
	else:
		#db.query("INSERT INTO deltas_neuro(topic,weigth) VALUES ('{}',{});".format(topicData[i][0],delta))
		db.query("UPDATE topics SET weight={} WHERE topic='{}'".format(delta,topicData[i][0]))
		delta = 0

i = len(topicData)-1
if topicData[i][0] == topicData[i-1][0]:
	delta = delta + topicData[i][1] + topicData[i-1][1]
elif (topicData[i][0] != topicData[i-1][0]) & (delta == 0):
	delta = topicData[i][1]
	#db.query("INSERT INTO deltas_neuro(topic,weigth) VALUES ('{}',{});".format(topicData[i][0],delta))
	db.query("UPDATE topics SET weight={} WHERE topic='{}'".format(delta,topicData[i][0]))
	delta = 0
else:
	#db.query("INSERT INTO deltas_neuro(topic,weigth) VALUES ('{}',{});".format(topicData[i][0],delta))
	db.query("UPDATE topics SET weight={} WHERE topic='{}'".format(delta,topicData[i][0]))
	delta = 0
