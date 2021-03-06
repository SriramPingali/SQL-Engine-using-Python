import csv
import sys
import sqlparse

class Database():
	def __init__(self):
		self.tables = {}

	def load_database(self, meta_file):
		tab=[]
		me=open(meta_file,'r')
		line=me.readlines()
		for i in range(0,len(line)):
			if line[i].strip('\n')=='<begin_table>':
				tab.append(line[i+1].strip('\n'))
		
		for i in range (0,len(tab)):
	
			temptable  = Table(tab[i])
			path = './'+tab[i]+".csv"

			temptable.load_table( path,meta_file)
			db.tables[tab[i]] = temptable	

		
			


class Table():
	def __init__(self, name):
		self.name = name
		self.columns = []
		self.rows = []

	def load_table(self, csvfile, meta_file):
		with open(meta_file, 'r') as meta_file:
			meta_reader = meta_file.readlines()
			for i in range(0, len(meta_reader)):
				if meta_reader[i].strip('\n') == self.name:
					i = i + 1 
					line = meta_reader[i].strip('\n')
					while line != "<end_table>":
						self.columns.append(line)
						i = i + 1
						line = meta_reader[i].strip('\n')
					break

		with open(csvfile, 'r') as csv_file:
		    csv_reader = csv.reader(csv_file)
		    line_count = 0
		    for row in csv_reader:
		    	row_dict = {}
		    	for i, col in enumerate(self.columns):
		    		row_dict[col] = int(row[i])
		    	self.rows.append(row_dict)


class Query():
	def __init__(self, query_str):
		self.query_str = query_str
		self.parse = None
		self.columns = []
		self.tables = []
		self.conditions = {}

	def query_processing(self):
		self.parse = sqlparse.format(self.query_str, reindent=True, keyword_case='upper').split('\n')
	#	print(self.parse[0].split()[1])
		for i in range(len(self.parse)):
			if "SELECT" in self.parse[i]:
				if "," in self.parse[i]:
					self.columns.append(self.parse[i].split()[1].strip(','))
					while "," in self.parse[i]:
						i = i + 1	
						self.columns.append(self.parse[i].strip(',').strip())
				else:		
					self.columns.append(self.parse[i].split()[1])
			elif "FROM" in self.parse[i]:
				self.tables.append(self.parse[i].split()[1])
				

	def execute(self):
		for tab in self.tables:
			for col in self.columns:
				if col == "*":
					print([list(x.values()) for x in db.tables[tab].rows])
				elif "max" in col:
					temp = col.replace("max(", "").replace(")", "")
					print(max([x[temp] for x in db.tables[tab].rows]))
				elif "sum" in col:
					temp = col.replace("sum(", "").replace(")", "")
					print(sum([x[temp] for x in db.tables[tab].rows]))
				elif "avg" in col:
					temp = col.replace("avg(", "").replace(")", "")
					print(sum([x[temp] for x in db.tables[tab].rows]) / 
							len([x[temp] for x in db.tables[tab].rows]))
				elif "min" in col:
					temp = col.replace("min(", "").replace(")", "")
					print(min([x[temp] for x in db.tables[tab].rows]))
				elif "count" in col:
					temp = col.replace("count(", "").replace(")", "")
					print(len([x[temp] for x in db.tables[tab].rows]))
				else:
					print([x[col] for x in db.tables[tab].rows])

if __name__ == '__main__':
	db = Database()
	db.load_database("./metadata.txt")
	
	quer = sys.argv[1]
	quer=str(quer)
	if quer[-1]!=';':
		raise Exception("error")
		
	else :
		quer=quer.replace(";","")
		
	
	qry = Query(quer)
	qry.query_processing()
	qry.execute()	
