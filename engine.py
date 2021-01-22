import csv
import sqlparse

class Database():
	def __init__(self):
		self.tables = {}

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

if __name__ == '__main__':
	table1 = Table("table1")
	table1.load_table('./files/table1.csv', './files/metadata.txt')

	table2 = Table("table2")
	table2.load_table('./files/table2.csv', './files/metadata.txt')

	db = Database()
	db.tables["table1"] = table1
	db.tables["table2"] = table2

	qry = Query("Select A, B, C from table1, table2")
	qry.query_processing()

	for tab in qry.tables:
		for col in qry.columns:
			if col == "*":
				print([list(x.values()) for x in db.tables[tab].rows])
			else:
				print([x[col] for x in db.tables[tab].rows])