import sqlparse
from sqlparse.sql import Where,Statement,Token,IdentifierList,Parenthesis,Identifier,Assignment,Function
import csv
import sys
from itertools import product 
from utilities import handle_error,handle_metadata



class TableData(object):
	"""docstring for TableData"""
	def __init__(self, metadata):
		super(TableData, self).__init__()
		self.metadata = metadata
		self.table_col_map = {}
		self.tablesName = []
		self.columnsName = []
		self.curr_columns = []
		self.columns = []
		for table in self.metadata:
			self.table_col_map[table.table_name] = table.col_names 
			self.tablesName.append(table.table_name)
			self.columnsName.append(table.col_names)

	def create_init_table(self):
		if len(self.tables) != 1:
			self.table = list(product(*self.tables))
		else:
			self.table = self.tables[0]
		# print(len(self.table),self.curr_columns)
		
	def get_operand(self,row_table,col_name):
		row = 0
		# print(row_table,col_name,self.curr_columns)
		if len(self.curr_table_names) >1:
			for colName in self.curr_columns:
				col = 0
				for column in colName:
					if column == col_name:
						return int(row_table[row][col])
					col+=1
				row+=1
		else:
			col = 0
			for column in self.curr_columns[0]:
				if column == col_name:
					return int(row_table[col])
				col+=1
		handle_error(4)
	
	def handle_distinct(self,columns):
		unique_values = {}
		table = []
		for row in self.table:
			temp_row = []
			for col in columns:
				temp_row.append(self.get_operand(row,col))
			try:
				if unique_values[tuple(temp_row)] == 1:
					pass
			except:
				table.append(row)
				unique_values[tuple(temp_row)] = 1
		self.table = table
				
	def calculate(self,op1,op,op2):
		"""
		"""
		if op == ">":
			return int(op1) > int(op2)
		elif op == ">=":
			return int(op1) >= int(op2)
		elif op == "<=":
			return int(op1) <= int(op2)
		elif op == "<":
			return int(op1) < int(op2)
		elif op == "=":
			# print(int(op1) == int(op2),op2,op1)
			return int(op1) == int(op2)
		else:
			handle_error(5)


	def apply_where_cond(self,where_tokens):
		col1,op,col2,int1,logic,col3,op2,col4,int2=where_tokens
		new_table = []
		if logic == "":
			if int1 != "":
				for row in self.table:
					if self.calculate(self.get_operand(row,col1),op,int1):
						new_table.append(row)
			else:
				for row in self.table:
					if self.calculate(self.get_operand(row,col1),op,self.get_operand(row,col2)): # need to implement calculate function
						new_table.append(row)
		else:
			if logic == "AND":
				if int1 == "" and int2 == "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,self.get_operand(row,col2)) and self.calculate(self.get_operand(row,col3),op2,self.get_operand(row,col4)):
							new_table.append(row)
				elif int1 == "" and int2 != "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,self.get_operand(row,col2)) and self.calculate(self.get_operand(row,col3),op2,int2):
							new_table.append(row)
				elif int1 != "" and int2 == "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,int1) and self.calculate(self.get_operand(row,col3),op2,self.get_operand(row,col4)):
							new_table.append(row)
				elif int1 != "" and int2 != "":
					for row in self.table:
						# print(self.get_operand(row,col1),int1,self.get_operand(row,col3),int2)
						if self.calculate(self.get_operand(row,col1),op,int1) and self.calculate(self.get_operand(row,col3),op2,int2):
							new_table.append(row)
			elif logic == "OR":
				if int1 == "" and int2 == "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,self.get_operand(row,col2)) or self.calculate(self.get_operand(row,col3),op2,self.get_operand(row,col4)):
							new_table.append(row)
				elif int1 == "" and int2 != "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,self.get_operand(row,col2)) or self.calculate(self.get_operand(row,col3),op2,int2):
							new_table.append(row)
				elif int1 != "" and int2 == "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,int1) or self.calculate(self.get_operand(row,col3),op2,self.get_operand(row,col4)):
							new_table.append(row)
				elif int1 != "" and int2 != "":
					for row in self.table:
						if self.calculate(self.get_operand(row,col1),op,int1) or self.calculate(self.get_operand(row,col3),op2,int2):
							new_table.append(row)
		self.table = new_table
	
				
				
	def apply_group_by(self,col_name,aggregate_func="",aggregate_func_col=""):
		buckets = {}
		row_index = 1
		for row in self.table:
			# print(row)
			try:
				buckets[self.get_operand(row,col_name)].append(row_index)
			except:
				buckets[self.get_operand(row,col_name)] = []
				buckets[self.get_operand(row,col_name)].append(row_index)
			row_index+=1
		# print(buckets)
		new_table = []
		for row_index in buckets:
			if aggregate_func == "COUNT":
				self.columns = [col_name,"count"]
				new_table.append([row_index,len(buckets[row_index])])
			elif aggregate_func == "AVG":
				self.columns = [col_name,"avg"]
				temp_sum = 0
				for rIndex in row_index:
					temp_sum = temp_sum + self.get_operand(self.table[rIndex-1],aggregate_func_col)
				new_table.append([row_index,temp_sum/len(row_index)])
			elif aggregate_func == "SUM":
				self.columns = [col_name,"sum"]
				temp_sum = 0
				for rIndex in row_index:
					temp_sum = temp_sum + self.get_operand(self.table[rIndex-1],aggregate_func_col)
				new_table.append([row_index,temp_sum])
			elif aggregate_func == "MIN":
				self.columns = [col_name,"min"]
				min_val = 100000000
				for rIndex in row_index:
					min_val = min(min_val,self.get_operand(self.table[rIndex-1],aggregate_func_col))
				new_table.append([row_index,min_val])
			elif aggregate_func == "MAX":
				self.columns = [col_name,"max"]
				max_val = -100000000
				for rIndex in row_index:
					max_val = max(max_val,self.get_operand(self.table[rIndex-1],aggregate_func_col))
				new_table.append([row_index,max_val])
			else:
				for rIndex in buckets[row_index]: 
					new_table.append(self.table[rIndex-1])


		self.table = new_table

	def aggregate_func(self,aggregate_func,aggregate_func_col):
		"""
		
		"""
		
		if aggregate_func == "COUNT":
			count = len(self.table)
			column_name = "COUNT("+str(aggregate_func_col)+")"
			self.columns.append(column_name)
			self.table = []
			self.table.append([count])
		elif aggregate_func == "MIN":
			min_val = 100000000
			for row in self.table:
				min_val = min(min_val,self.get_operand(row,aggregate_func_col))
			column_name = "MIN("+str(aggregate_func_col)+")"
			self.columns.append(column_name)
			self.table = []
			self.table.append([min_val])
		elif aggregate_func == "MAX":
			max_val = -100000000
			for row in self.table:
				max_val = max(max_val,self.get_operand(row,aggregate_func_col))
			column_name = "MAX("+str(aggregate_func_col)+")"
			self.columns.append(column_name)
			self.table = []
			self.table.append([max_val])
		elif aggregate_func == "AVG":
			sum_val = 0
			for row in self.table:
				sum_val = sum_val + self.get_operand(row,aggregate_func_col)
			avg_val = sum_val/len(self.table)
			column_name = "AVG("+str(aggregate_func_col)+")"
			self.columns.append(column_name)
			self.table = []
			self.table.append([avg_val]) 
		elif aggregate_func == "SUM":
			sum_val = 0
			for row in self.table:
				sum_val = sum_val + self.get_operand(row,aggregate_func_col)
			column_name = "SUM("+str(aggregate_func_col)+")"
			self.columns.append(column_name)
			self.table = []
			self.table.append([sum_val])

	def select_columns(self,col_names):
		"""
		select columns to print
		"""
		for col_name in col_names:
			print(col_name),
		for row in self.table:
			print(row)

	def apply_order_by(self,col_name):
		"""
		order by just one columns
		"""
		if "DESC" in col_name:
			col_name = col_name.replace("DESC","")
			col_name = col_name.replace(" ","")
			order = "DESC"
		else:
			col_name = col_name.replace("ASC","")
			col_name = col_name.replace(" ","")
			order = "ASC"

		if order != "DESC":
			for i in range(0,len(self.table)):
				for j in range(0,len(self.table)-i-1):
					if self.get_operand(self.table[j],col_name) > self.get_operand(self.table[j+1],col_name):
						self.table[j],self.table[j+1] = self.table[j+1],self.table[j]
		else:
			for i in range(0,len(self.table)):
				for j in range(0,len(self.table)-i-1):
					if self.get_operand(self.table[j],col_name) < self.get_operand(self.table[j+1],col_name):
						self.table[j],self.table[j+1] = self.table[j+1],self.table[j]

	def select_tables(self,table_names):
		self.csv_files = []
		self.curr_table_names = table_names
		for table in table_names:
			csv_file = open(str(table)+".csv", newline='')
			self.csv_files.append(csv.DictReader(csv_file,fieldnames=self.table_col_map[table]))
			self.curr_columns.append(self.table_col_map[table])
		self.tables = []
		for i in range(len(self.csv_files)):
			table = []
			for row in self.csv_files[i]:
				table.append(list(row.values()))
			self.tables.append(table)
	
	def project(self,col_names,agg_flag):
		# print(len(col_names),agg_flag)
		if len(col_names) == 1 and agg_flag != True:
			if col_names[0] == "*":
				for table in self.curr_table_names:
					for col_name in self.table_col_map[table]:
						print(str(table)+"."+col_name+" ",end=" ") # change table name to reflect cases where mutiple tables are involved
				print("")
				if len(self.curr_table_names) != 1:
					for row in self.table:
						for col in row:
							for col_value in col:
								print(col_value,end=" ")
						print("")
				else:
					for row in self.table:
						for col in row:
							print(col,end=" ")
						print("")

						
			else:
				print(str(col_names[0]))
				for row in self.table:
					print(self.get_operand(row,col_names[0]))
		elif len(col_names) == 1 and agg_flag == True:
			if col_names[0] == "*":
				print(self.columns[0])
				print(self.table[0][0])
			else:
				print(self.columns[0])
				print(self.table[0][0])
		elif len(col_names) > 1:
			# for table in self.curr_table_names:
			# 		for col_name in self.table_col_map[table]:
			# 			print(str(table)+"."+col_name+"     ",end=" ") # change table name to reflect cases where mutiple tables are involved
			# 	print("\n")
			for col_name in col_names:
				print(col_name,end=" ")
			print("")
			for row in self.table:
				for col in row:
					print(col,end=" ")
				print("")
			


class Access_plan(object):
		""" parser class error handling"""
		def __init__(self,sql, metadata):
			super(Access_plan, self).__init__()
			self.metadata = metadata
			self.parsed = ""
			self.sql_statement = sql
			self.table_names = []
			self.col_names = []
			self.group_by_cols = []
			self.order_by_cols = []
			self.where_token = "" 
			self.where_condition = False
			self.group_by = False
			self.select = False
			self.order_by = False
			self.aggregate_func_flag = False # This is when there is no where condition but aggregate function exists
			self.distinct_flag = False

		def sql_parsing(self):
			# self.parsed = sqlparse.parse(sql_statement)
			self.sql_parsing_helper()
			# handle_error(parsed)
			if self.where_condition == True :
				self.handle_where()
			# if self.group_by == True:
			# 	self.handle_group_by(self.group_by_cols)
			# if self.select == True:
			# 	self.handle_select(self.col_names)
			# if self.order_by == True:
			# 	self.handle_order_by(self.order_by_cols)

		def sql_parsing_helper(self):
			self.sql_statement = self.sql_statement.strip(" ")
			if self.sql_statement[-1] != ";":
				handle_error(1)
			self.sql_statement = self.sql_statement[:-1]
			formatted_sql = sqlparse.format(self.sql_statement,keyword_case='upper')
			self.parsed = sqlparse.parse(formatted_sql)
			select_flag = False
			from_flag = False
			where_flag = False
			group_by_flag = False
			order_by_flag = False
			distinct_flag = False
			group_by_keyword = False
			order_by_keyword = False
			if self.parsed[0].tokens[0].value != "SELECT":
				handle_error(2)
			for token in self.parsed[0].tokens:
				# print(token.ttype,isinstance(token,Identifier),token.value)
				# if token.ttype == None:
				if distinct_flag == False and str(token.value) == "DISTINCT":
					distinct_flag = True
					self.distinct_flag = True
				elif select_flag == False and (isinstance(token,IdentifierList) or isinstance(token,Identifier) or isinstance(token,Function))  and token.ttype == None or str(token.ttype) == "Token.Wildcard":
					select_flag = True
					if isinstance(token,IdentifierList):
						identifier_gen = token.get_identifiers()
						while True:
							try:
								self.col_names.append(next(identifier_gen).value)
							except:
								break
					else:
						self.col_names.append(token.value)
					if len(self.col_names) == 1:
						for agg_fun in ["COUNT","SUM","MIN","MAX","AVG"]:
							if agg_fun in self.col_names[0].upper():
								self.aggregate_func_flag = True
								s = results.col_names[0]
								self.col_names = []
								self.col_names.append(s[s.find("(")+1:s.find(")")])
								self.agg_fun = agg_fun
								break
				elif from_flag == False and (isinstance(token,IdentifierList) or isinstance(token,Identifier)) and token.ttype == None:
					from_flag = True
					if isinstance(token,IdentifierList):
						identifier_gen = token.get_identifiers()
						while True:
							try:
								self.table_names.append(next(identifier_gen).value)
							except:
								break
					else:
						self.table_names.append(token.value)
				elif where_flag == False and isinstance(token,Where) and token.ttype == None:
					where_flag = True
					self.where_condition = True
					self.where_token = token
				elif group_by_flag == False and group_by_keyword == True and (isinstance(token,IdentifierList) or isinstance(token,Identifier)) and token.ttype == None:
					group_by_flag = True
					self.group_by = True
					if isinstance(token,IdentifierList):
						identifier_gen = token.get_identifiers()
						while True:
							try:
								self.group_by_cols.append(next(identifier_gen).value)
							except:
								break
					else:
						self.group_by_cols.append(token.value)
				elif order_by_flag == False and order_by_keyword == True and (isinstance(token,IdentifierList) or isinstance(token,Identifier)) and token.ttype == None:
					order_by_flag = True
					self.order_by = True
					if isinstance(token,IdentifierList):
						identifier_gen = token.get_identifiers()
						while True:
							try:
								self.order_by_cols.append(next(identifier_gen).value)
							except:
								break
					else:
						self.order_by_cols.append(token.value)
				elif group_by_keyword == False and token.value == "GROUP BY":
					group_by_keyword = True 
				elif order_by_keyword == False and token.value == "ORDER BY":
					order_by_keyword = True

		def handle_where(self):
			# print(self.where_token.value.strip("WHERE "))	
			where = self.where_token.value.strip("WHERE ")
			if " AND " in where:
				left,right = where.split(" AND ",1)
				for op in ["<=", ">=","<" , ">", "="]:
					if op in left:
						left_left,left_right = left.split(op,1)
						left_right = left_right.replace(" ","")
						left_left = left_left.replace(" ","")
						operator = op
						break
				for op in ["<=", ">=","<" , ">", "="]:
					if op in right:
						right_left,right_right = right.split(op,1)
						operator2 = op
						right_left = right_left.replace(" ","")
						right_right =right_right.replace(" ","")
						break
				if not right_right.isnumeric() and not left_right.isnumeric():
					self.where_tokens = [left_left,operator,left_right,"","AND",right_left,operator,right_right,""]
				elif right_right.isnumeric() and not left_right.isnumeric():
					self.where_tokens = [left_left,operator,"",left_right,"AND",right_left,operator,right_right,""]
				elif not right_right.isnumeric() and left_right.isnumeric():
					self.where_tokens = [left_left,operator,left_right,"","AND",right_left,operator,"",right_right]
				else:
					self.where_tokens = [left_left,operator,"",left_right,"AND",right_left,operator,"",right_right]

			elif " OR " in where:
				left,right = where.split(" OR ",1)
				for op in ["<=", ">=","<" , ">", "="]:
					if op in left:
						left_left,left_right = left.split(op,1)
						left_right = left_right.replace(" ","")
						left_left = left_left.replace(" ","")
						operator = op
						break
				for op in ["<=", ">=","<" , ">", "="]:
					if op in right:
						right_left,right_right = right.split(op,1)
						operator2 = op
						right_left = right_left.replace(" ","")
						right_right =right_right.replace(" ","")
						break
				if not right_right.isnumeric() and not left_right.isnumeric():
					self.where_tokens = [left_left,operator,left_right,"","OR",right_left,operator,right_right,""]
				elif right_right.isnumeric() and not left_right.isnumeric():
					self.where_tokens = [left_left,operator,"",left_right,"OR",right_left,operator,right_right,""]
				elif not right_right.isnumeric() and left_right.isnumeric():
					self.where_tokens = [left_left,operator,left_right,"","OR",right_left,operator,"",right_right]
				else:
					self.where_tokens = [left_left,operator,"",left_right,"OR",right_left,operator,"",right_right]
			else:
				for op in ["<=", ">=","<" , ">", "="]:
					if op in where:
						left_left,left_right = where.split(op,1)
						left_right = left_right.replace(" ","")
						left_left = left_left.replace(" ","")
						operator = op
						break
				if not left_right.isnumeric():
					self.where_tokens = [left_left,operator,left_right,"","","","","",""]
				else:
					self.where_tokens = [left_left,operator,"",left_right,"","","","",""]

			# print(left,right)
			# print(left_left.replace(" ",""),left_right.replace(" ",""),right_left.replace(" ",""),right_right.replace(" ",""))

		def select_tables(self,table_names):
			self.csv_files = []
			for table in table_names:
				for tb in self.metadata:
					if tb.table_name == table:
						fieldnames = tb.col_names

				csv_file = open(table+".csv", newline='')
				self.csv_files.append(csv.DictReader(csv_file,fieldnames=fieldnames)) 


if __name__ == "__main__":
	# print("hello")
	metadata = handle_metadata("metadata.txt")
	sql = str(sys.argv[1])
	results = Access_plan(sql,metadata)
	results.sql_parsing()
	# print(results.table_names)
	# print(results.col_names)
	# print(results.aggregate_func_flag)
	pr = TableData(metadata)
	for tab in results.table_names:
		tab_flag = False
		for table in metadata:
			if table.table_name == tab:
				tab_flag = True
		if not tab_flag:
			handle_error(3)
	pr.select_tables(results.table_names)
	pr.create_init_table()
	if results.where_condition:
		# print(results.where_tokens)
		pr.apply_where_cond(results.where_tokens)
	if results.group_by:
		for col_name in results.col_names:
			for agg_fun in ["COUNT","SUM","MIN","MAX","AVG"]:
				if agg_fun in col_name.upper():
					s = col_name
					col_names = s[s.find("(")+1:s.find(")")]
					agg_function = agg_fun
					break
		pr.apply_group_by(results.group_by_cols[0],agg_function,col_names)
	if results.distinct_flag:
		pr.handle_distinct(results.col_names)
	if results.aggregate_func_flag:
		pr.aggregate_func(results.agg_fun,results.col_names[0])
	# print("order by flag",results.order_by_cols)
	if results.order_by:
		pr.apply_order_by(results.order_by_cols[0])
	pr.project(results.col_names,results.aggregate_func_flag)

