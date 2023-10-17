import mysql.connector
import os
import threading
import sys
import numpy as np
import datetime
import pandas as pd
import plotly.graph_objects as go
import websocket
import _thread
import time
import json
from binance.client import Client

databasename="crypthonws"
mydb=0
eventType="@trade"	#"@kline_1s"
compteurdebug=0

def connectdb(dbname) :
	print("connectdb(dbname)")
	mydb = mysql.connector.connect(host="localhost", database=dbname, user="...", password="...")
	return mydb
	
mydb=connectdb(databasename) 	

def info_():
	api_key = "..."
	api_secret = "..."
	client = Client(api_key, api_secret)
	exchange_info = client.get_exchange_info()
	lexcinfo=[]
	linfo=[]
	i=0
	for s in exchange_info['symbols']:
		if s['symbol'].lower().endswith("usdt"):
			linfo.append(s['symbol'].lower())
			lexcinfo.append(s['symbol'].lower()+eventType)
			if i>=50: break
			i+=1
	req_='{ "method": "SUBSCRIBE", "params": ' + str(lexcinfo).replace("'","\"") + ', "id": 1}'
	return req_

requete_=info_()


def listtable(mydb) :
	print("list of table : ")
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	s=""
	for c in mycursor:
		print(s.join(c))
listtable(connectdb(databasename))


# weight : Quantity
def ws_message (ws,message):
	#print(message)
	m=json.loads(message)
	#print("json : ",m)
	if m['e']=='kline':
		#print(m['s'],m['E'],m['k']['q'],m['k']['c'])
		compute(m['s'],m['E'],m['k']['q'],m['k']['c'])
	if m['e']=='trade':
		#print(m['s'],m['E'],m['q'],m['p'])
		compute(m['s'],m['E'],m['q'],m['p'])
			
def ws_open(ws):
	print(requete_)
	ws.send(requete_)
	
# weight : Quantity
def ws_thread(*args):
	#print("def ws_thread(*args):")
	ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws", on_open = ws_open, on_message = ws_message)
	ws.run_forever()

		
def deleteAllFromOneTable(mydb,table_name) :
	mycursor = mydb.cursor()
	table_name="DELETE FROM "+table_name+" ;"
	mycursor.execute(table_name)
	mydb.commit()

def selectLastTuple(table_name):
	mycursor = mydb.cursor()
	table_name="SELECT * FROM "+table_name+" ORDER BY timestamp DESC;"	#+" WHERE timestamp = MAX(timestamp);"
	mycursor.execute(table_name)
	myresult = mycursor.fetchone()
	print(myresult)
	return myresult


def dropAllTables(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	ltable=[]
	s=""
	for c in mycursor:
		print(c)
		#print(type(c))	<class 'tuple'>
		ltable.append(s.join(c))
	for t in ltable:
		sql = "DROP TABLE "+t
		mycursor.execute(sql) 

def dropAllTablesIf(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	ltable=[]
	for c in mycursor:
		s=""
		s=s.join(c)
		if "rsi" in s or "RSI" in s:
			print(s)
			ltable.append(s)
	for t in ltable:
		sql = "DROP TABLE "+t
		mycursor.execute(sql) 
		
def dropOneTable(mydb,t) :
	mycursor = mydb.cursor()
	sql = "DROP TABLE "+t
	mycursor.execute(sql) 	
		
def selectAllFrom1Table(mydb,table_name) :
	mycursor = mydb.cursor()
	table_name="SELECT * FROM "+table_name
	mycursor.execute(table_name)
	myresult = mycursor.fetchall()
	for r in myresult:
  		print(r)

def selectallfromalltables(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	myresult = mycursor.fetchall()
	for r in myresult:
		s=''
		selectAllFrom1Table(mydb,s.join(r))
		
def connect_db() :
	mydb = mysql.connector.connect(host="localhost",user="...",password="...")
	return mydb

def insertCoin(listToDB):
	#print("insertCoin : ",listToDB)
	mycursor = mydb.cursor()
	sql = "INSERT INTO coin" + listToDB[0] + " ( coin , timestamp , weight , price , timea , timeb , timec ,  rsimma , volumea , volumeb , volumec ,  sma7 , varsma7 , signvarsma7 , ranksma7 , varranksma7 , sma8 , varsma8 , signvarsma8 , ranksma8 , varranksma8 , sma9 , varsma9 , signvarsma9 , ranksma9 , varranksma9 , sma20 , varsma20 , signvarsma20 , ranksma20 , varranksma20 , sma50 , varsma50 , signvarsma50 , ranksma50 , varranksma50 , sma200 , varsma200 , signvarsma200 , ranksma200 , varranksma200 , pricetwin , varprice , signvarsmaprice , rankprice , varrankprice ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	#print("sql : ",sql)
	val = (listToDB[0], listToDB[1], listToDB[2], listToDB[3], listToDB[4], listToDB[5], listToDB[6], listToDB[7], listToDB[8], listToDB[9], listToDB[10], listToDB[11], listToDB[12], listToDB[13], listToDB[14], listToDB[15], listToDB[16], listToDB[17], listToDB[18], listToDB[19], listToDB[20], listToDB[21], listToDB[22], listToDB[23], listToDB[24], listToDB[25], listToDB[26], listToDB[27], listToDB[28], listToDB[29], listToDB[30], listToDB[31], listToDB[32], listToDB[33], listToDB[34], listToDB[35], listToDB[36], listToDB[37], listToDB[38], listToDB[39], listToDB[40], listToDB[41], listToDB[42], listToDB[43], listToDB[44], listToDB[45])
	#print("val : ",val)
	mycursor.execute(sql, val)
	mydb.commit()

def insertCoinTemp(listToDB):
	print("insertCoinTemp : ",listToDB)
	mycursor = mydb.cursor()
	sql = "INSERT INTO temptable ( coin , timestamp , weight , price , timea , timeb , timec ,  rsimma , volumea , volumeb , volumec ,  sma7 , varsma7 , signvarsma7 , ranksma7 , varranksma7 , sma8 , varsma8 , signvarsma8 , ranksma8 , varranksma8 , sma9 , varsma9 , signvarsma9 , ranksma9 , varranksma9 , sma20 , varsma20 , signvarsma20 , ranksma20 , varranksma20 , sma50 , varsma50 , signvarsma50 , ranksma50 , varranksma50 , sma200 , varsma200 , signvarsma200 , ranksma200 , varranksma200 , pricetwin , varprice , signvarsmaprice , rankprice , varrankprice ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	#print("sql : ",sql)
	val = (listToDB[0], listToDB[1], listToDB[2], listToDB[3], listToDB[4], listToDB[5], listToDB[6], listToDB[7], listToDB[8], listToDB[9], listToDB[10], listToDB[11], listToDB[12], listToDB[13], listToDB[14], listToDB[15], listToDB[16], listToDB[17], listToDB[18], listToDB[19], listToDB[20], listToDB[21], listToDB[22], listToDB[23], listToDB[24], listToDB[25], listToDB[26], listToDB[27], listToDB[28], listToDB[29], listToDB[30], listToDB[31], listToDB[32], listToDB[33], listToDB[34], listToDB[35], listToDB[36], listToDB[37], listToDB[38], listToDB[39], listToDB[40], listToDB[41], listToDB[42], listToDB[43], listToDB[44], listToDB[45])
	#print("val : ",val)
	mycursor.execute(sql, val)
	mydb.commit()

def myfunc(result):
	return result[1]
# timea = listToDB[04] = datetime
def compute(coin,timestamp,weight,price):
	global compteurdebug
	timest=str(timestamp)[-9:]
	tnow=datetime.datetime.now()
	#listToDB[5]
	print(str(tnow))
	listToDB=[coin,timest,round(float(weight),6),round(float(price),6),str(tnow)]
	print("coin",listToDB[0],"time",listToDB[1],"volume",listToDB[2],"price",listToDB[3],"date",listToDB[4])
	for i in range(41):
		if i==36:
			listToDB.append(round(float(price),6))
		else:
			listToDB.append(0)
	
	mycursor = mydb.cursor()
	
	#print ("_ listToDB",listToDB)
	table_name="SELECT * FROM coin"+coin+" ORDER BY timestamp DESC LIMIT 200 ;"	# case sensitive !
	#print("tablname",table_name,coin)
	mycursor.execute(table_name)
	ultat=mycursor.fetchall()
	#print ("_ ultat : ",ultat)
	myresult=[]
	myresult.append(tuple(listToDB))
	#print ("_ _ myresult : ",myresult)
	myresult+=ultat
	#print ("_ _ _ myresult : ",myresult)

	myresult.sort(key=myfunc)
	#sorted(myresult, key=lambda listToDB: listToDB[1]) 

	#print ("_ myresult.sort",myresult)
	delta=14
	epsilon=1
	theta=40
	epsilonema21=2

	derncoursh=derncoursb=derncourshtheta=derncoursbtheta=0
	b=bo=False
	mmeh=mmeb=0
	lrsihausses=[]
	lrsibaisses=[]
	lrsihaussestheta=[]
	lrsibaissestheta=[]
	rsi=rsitheta=0
	m=0

	ema21=mma9=mma8=mma7=mma20=mma50=mma200=0

	#print ("listToDB",listToDB)
	
	if len(myresult)>=200:
		for x in myresult:
				#print(x)
				if m>0:
					mma200+=float(x[3])
				
				if m>151:
					mma50+=float(x[3])
					
				nrsihh=0
				nrsibb=0
				nrsih=nrsib=mhb=rsi=0
				nrsihtheta=nrsibtheta=mhbtheta=rsitheta=0
								
				if m>(200-theta):
					nrsitheta=float(x[3])-float(y[3])	#x[3] price
					#print("hausse ou baisse : "+str(nrsitheta))
					if nrsitheta>=0:
						lrsihaussestheta.append(nrsitheta)
						lrsibaissestheta.append(0)
					else:
						lrsibaissestheta.append(-nrsitheta)
						lrsihaussestheta.append(0)
						
				if m>179:
					ema21+=float(x[3])
					
				if m>180:
					mma20+=float(x[3])
					
				if m>(200-delta):
					nrsi=float(x[3])-float(y[3])	#x[3] price
					#print("hausse ou baisse : "+str(nrsi))
					if nrsi>=0:
						lrsihausses.append(nrsi)
						lrsibaisses.append(0)
					else:
						lrsibaisses.append(-nrsi)
						lrsihausses.append(0)
				
				if m>191:
					mma9+=float(x[3])
				if m>192:
					mma8+=float(x[3])
				if m>193:
					mma7+=float(x[3])
						
				if m>=(200):
					nrsih=nrsib=0
					for mm in range(len(lrsihausses)):	#range(m-delta,m):
						nrsih=float(lrsihausses[mm])+nrsih					
						nrsib=float(lrsibaisses[mm])+nrsib					
						derncoursh=lrsihausses[mm]
						derncoursb=lrsibaisses[mm]
					#nrsih=nrsih/14
					#mme=(mme*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*dercours)
					if b==False:
						b=True
						mmeh=nrsih=nrsih/delta
						mmeb=nrsib=nrsib/delta
					else:
						mmeh=nrsih=(mmeh*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*derncoursh)
						mmeb=nrsib=(mmeb*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*derncoursb)
					mhb=0
					if nrsib!=0:
						mhb=nrsih/nrsib
					rsi=100-(100/(1+mhb))
					#print("coin:",x[0]," date/heure:",x[1]," cours:",x[3]," rsi:",rsi)
					#pause=input("pause")

					
					nrsihtheta=nrsibtheta=0
					for mmtheta in range(len(lrsihaussestheta)):	#range(m-delta,m):
						nrsihtheta=float(lrsihaussestheta[mmtheta])+nrsihtheta					
						nrsibtheta=float(lrsibaissestheta[mmtheta])+nrsibtheta					
						derncourshtheta=lrsihaussestheta[mmtheta]
						derncoursbtheta=lrsibaissestheta[mmtheta]
					#nrsih=nrsih/14
					#mme=(mme*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*dercours)
					if bo==False:
						bo=True
						mmehtheta=nrsihtheta=nrsihtheta/theta
						mmebtheta=nrsibtheta=nrsibtheta/theta
					else:
						mmehtheta=nrsihtheta=(mmehtheta*(1-(epsilon/(theta+epsilon-1))))+((epsilon/(theta+epsilon-1))*derncourshtheta)
						mmebtheta=nrsibtheta=(mmebtheta*(1-(epsilon/(theta+epsilon-1))))+((epsilon/(theta+epsilon-1))*derncoursbtheta)
					mhbtheta=0
					if nrsibtheta!=0:
						mhbtheta=nrsihtheta/nrsibtheta
					rsitheta=100-(100/(1+mhbtheta))
					#print("coin:",x[0]," date/heure:",x[1]," cours:",x[3]," rsi:",rsi)
					#pause=input("pause")


				y=x
				m+=1


	mma9/=9
	mma8/=8
	mma7/=7
	mma20/=20
	mma50/=50
	mma200/=200
	ema21=(ema21+float(listToDB[41])*(epsilonema21-1))/(21+epsilonema21-1)
	listToDB[9]=round(ema21,6)
	listToDB[21]=str(round(mma9,6))
	listToDB[16]=str(round(mma8,6))
	listToDB[11]=str(round(mma7,6))
	listToDB[26]=str(round(mma20,6))
	listToDB[31]=str(round(mma50,6))
	listToDB[36]=str(round(mma200,6))
	listToDB[7]=str(round(rsi,6))
	listToDB[8]=round(rsitheta,6)
	
	# listToDB[10] : variation de rsitheta, rsi40
	try:
		listToDB[10]=round((float(listToDB[8])-float(ultat[0][8])),6)
	except IndexError:
		listToDB[10]=round(float(listToDB[8]),6)
		
	rankdict={}
	# calcul des variations n - n-1
	for i in range(26,42,5):
		try:
			listToDB[i+1]=round(float(listToDB[i])-float(ultat[0][i]),6)
		except IndexError:
			listToDB[i+1]=round(float(listToDB[i]),6)
		#print(i,"listToDB[i+1]",round(float(listToDB[i]),6))
		# calcul du rank
		rankdict.update({ i+3 : listToDB[i] })
		# calcul signe now / before
		try:
			if np.sign(float(listToDB[i+1])) != np.sign(float(ultat[0][i+1])):
				listToDB[i+2]=int(np.sign(float(listToDB[i+1])))
			else:
				listToDB[i+2]=0
		except IndexError:
			listToDB[i+2]=0
		

	rankdict_=dict(sorted(rankdict.items(), key=lambda item: float(item[1])))
	#print(rankdict_)
	j=0
	for x in rankdict_:
		listToDB[x]=int(j)
		#print(x,listToDB[x])
		j+=1
		
	seuil=66
	compteurdebug+=1

	if compteurdebug>200:	# pour les tests...sinon faire tourner à vide puis après 200 enregistr. lancer tests
	
		#fp=open("journal.csv", 'a')
		
		print("évènement déclencheur ACHAT , retournement du cours : ",listToDB[43]," au-dessus de toutes les mm : ",listToDB[44]," descente sma7 : ",listToDB[13])
		
		print("évènement déclencheur VENTE , rsi40>66 : ",listToDB[8]," descendant : ",listToDB[10])

		listlevel2=[]
		listlevel2.append(tuple(listToDB))
		listlevel2+=ultat
		
		if listToDB[43] > 0 and listToDB[44] == 3 and listToDB[13] == 0:
			#print(listToDB[3])
			print("\n____________________ACHAT coin : ",listToDB[0])	# on passe de 0 ou - à + = le cours monte

			#fp.write("200;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
			title="Entrée - coin : "+listToDB[0]+" timestamp : "+str(listToDB[1])
			listlevel21 = listlevel2.copy()
			t1=threading.Thread(target=graph,args=(listlevel21,title,))
			t1.start()
			#analyse(listlevel2)
			#pause=input("pause")
		if listToDB[8] > seuil and listToDB[10] <= 0:
			#print(listToDB[3])
			print("\n___________________________VENTE coin : ",listToDB[0])	# on passe de 0 ou - à + = le cours monte

			#fp.write("600;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
			title="Sortie - coin : "+listToDB[0]+" timestamp : "+str(listToDB[1])
			listlevel22 = listlevel2.copy()
			t2=threading.Thread(target=graph,args=(listlevel22,title,))
			t2.start()
			#analyse(listlevel2)
			#pause=input("pause")
		#else:
			#fp.write("400;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
		#pause=input("pause")
	#print("insertCoin(listToDB)",listToDB)
	insertCoin(listToDB)
	insertCoinTemp(listToDB)


def graph(l,title):
	lcours=[]
	lsma7=[]
	lsma20=[]
	lsma50=[]
	lsma200=[]
	l.sort(key=myfunc)
	ltime=[]
	i=0
	s=""
	tmstp=""
	for x in l:
		if i >= 150:
			lcours.append(round(float(x[3])))
			#s+=str(x[3])+" ; "
			lsma7.append(round(float(x[11])))
			#s+=str(x[11])+" ; "
			lsma20.append(round(float(x[26])))
			#s+=str(x[26])+" ; "
			lsma50.append(round(float(x[31])))
			#s+=str(x[31])+" ; "
			lsma200.append(round(float(x[36])))
			#s+=str(x[36])+" \n "
			ltime.append(i-200)
		i+=1
		tmstp=x[1]
		#print(s)	
		#pause=input("pause")
	#s=""
	f1 = go.Figure(data = [go.Scatter(x=ltime, y=lcours, name="cours"), go.Scatter(x=ltime, y=lsma7, name="sma7"), go.Scatter(x=ltime, y=lsma20, name="sma20"),  go.Scatter(x=ltime, y=lsma50, name="sma50"),  go.Scatter(x=ltime, y=lsma200, name="sma200")], layout = {"xaxis":{"title": "temps"},"yaxis":{"title": "valeurs"},"title":title})
	f1
	s=tmstp+"-lgraph.png"
	#f1.write_image(s)
    
#######################################################	

# Démarrer un nouveau thread pour l'interface WebSocket
_thread.start_new_thread(ws_thread, ())
			
while True:
	...=1

