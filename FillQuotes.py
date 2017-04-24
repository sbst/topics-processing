import postgresql
import csv

db = postgresql.open('')

with open('data.csv', newline='') as csvfile:
	datareader = csv.reader(csvfile, delimiter=';', quotechar='|')
	for row in datareader:	
		db.query("INSERT INTO public.quotations(pair, date, quatation) VALUES ('EURUSD', to_timestamp(\'{}\','yyyymmdd hh24miss') at time zone 'EST',{});".format(row[0], row[1]))
