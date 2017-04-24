import postgresql


db = postgresql.open('')
delta = 0
# topic_id  DELTA
# 32		-0.02
#for month in range(1,13):
query = "SELECT * FROM GETDELTACLOUDFIRST WHERE delta is not null"
topicData = db.query(query)
for i in range(0,len(topicData)):	
	db.query("UPDATE topic_cloud SET weight={} WHERE cloud_id={}".format(topicData[i][1],topicData[i][0]))
