import sys 

class Table:
	def __init__(self,table_name,column_names):
		self.table_name = table_name
		self.col_names = []
		for col_name in column_names:
			self.col_names.append(col_name)
	
	def check_col_name(column_name):
		for col_name in self.col_names:
			if(column_name == col_name):
				return True
		return False

def handle_error(error_id):
	if error_id == 1:
		print("ERROR!! SQL quries should end with ;")
	elif error_id == 2:
		print("ERROR!! only handles select quries")
	elif error_id == 3:
		print("ERROR!! Table does not exists in DataBase")
	elif error_id == 4:
		print("ERROR!! No column of this type exists")
	elif error_id == 5:
		print("ERROR!! Incorrect operator")
	else:
		print("ERROR!!")
	sys.exit()

def handle_metadata(file_name):
	metadata_file = open(file_name, "r")
	# print("hello")
	str_line = metadata_file.readline()
	table_db = []
	while(str_line != ""):
		# print(str_line,"<begin_table>")
		if(str_line == "<begin_table>\n"):
			table_name = metadata_file.readline()
			col_names = []
			while(str_line != "<end_table>\n"):
				str_line = metadata_file.readline() 
				if(str_line != "<end_table>\n"):
					col_names.append(str_line.rstrip())
			# print(table_name.rstrip(),col_names)
			table_db.append(Table(table_name.rstrip(),col_names)) 
		str_line = metadata_file.readline()
	return table_db