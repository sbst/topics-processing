from google.cloud import language

import psycopg2

conn = psycopg2.connect("dbname='' user='' host='' password=''")

cur = conn.cursor() 

iter = 0

cur.execute("SELECT news_id FROM public.news WHERE EXTRACT(MONTH FROM news_date)=2 AND news_id%10=0 ORDER BY news_id")
rows = cur.fetchall()
for row in rows:
	if iter == 30:
		iter = 0
		conn.commit()
	language_client = language.Client()
	cur.execute("SELECT text FROM public.news WHERE news_id=%s",((row,)))
	newsText = cur.fetchall()
	document = language_client.document_from_text(newsText[0][0])
	entities = document.analyze_entities().entities
	if len(entities) > 3:
		for i in range(0,4):
			cur.execute("INSERT INTO topics(topic, news) VALUES (%s, %s);",(entities[i].name, row[0]))
		cur.execute("SELECT topic_id FROM topics ORDER BY TOPIC_ID DESC LIMIT 4;")
		topicId = cur.fetchall()
		cur.execute("INSERT INTO topic_cloud(topic_1,topic_2,topic_3,topic_4) VALUES ({},{},{},{})".format(topicId[3][0],topicId[2][0],topicId[1][0],topicId[0][0]))
		cur.execute("SELECT max(cloud_id) FROM topic_cloud")
		maxTopicId = cur.fetchall()
		cur.execute("UPDATE news SET topic_cloud=%s WHERE news_id=%s", (maxTopicId[0][0], row[0]))
		iter = iter + 1

conn.commit()
cur.close()
conn.close()
