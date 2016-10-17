from datetime import datetime
import os
import argparse
import re


class Census(object):
	def __init__(self,target_dir,data_dir,):
		
		self.target_dir = target_dir
		self.data_dir = data_dir

		self.census_db = ""
		self.census_table_name = ""

		
	def set_dbs(self):
		self.census_db = os.path.join(self.data_dir, "census.sqlite")
		self.census_table_name = "census"
		if not os.path.exists(self.census_db):
			print "missing: ",self.census_db
			print "going on without it..."


	def fix_excel_sheet(self,path):
		from clean import clean
		clean_path = os.path.join(self.target_dir, "clean.csv")
		tmp = os.path.join(self.target_dir, "tmp.csv")
		cl = clean()
		cl.run(path,tmp,clean_path)
		return clean_path


def main():
	data_dir = "where's your data?"
	target_dir = "where to send the results?"
	parser = argparse.ArgumentParser()
	parser.add_argument("--query","-q",dest = "query",action = "store_true")
	parser.add_argument("--queryfile","-qf",dest = "queryfile",default = "census_query.txt")
	parser.add_argument("--sql",dest = "sql",default = "")
	parser.add_argument("--description","-dscr",dest = "description",action = "store_true")
	parser.add_argument("--std_out",dest = "std_out",action = "store_true")
	parser.add_argument("--csv",dest = "csv",default = "census_results.txt")
	parser.add_argument("--d2","-d",dest = "d2",action = "store_true")
	parser.add_argument("--d2_variable","-ddv",dest = "dd_variable",default = "DP0010001")
	parser.add_argument("--percentile","-pct",dest = "percentile",default = 90)
	parser.add_argument("--population_query","-pq",dest = "population_query")
	parser.add_argument("--location","-loc",dest = "location", default = '40.7305991,-73.9865812')
	parser.add_argument("--radius","-r",dest = "radius",default = 10)
	parser.add_argument("--pq_variable","-pqv",dest="pq_variable",default = "DP0010001")
	args = parser.parse_args()

	if args.query:
		census = Census(target_dir,data_dir)
		print args.sql
		if args.sql:
			from query import Query
			census.set_dbs()
			query = Query(census.census_db,census.census_table_name)
			if os.path.exists(args.sql):
				with open(args.sql) as f:
					sqls = f.read().split('\n')
			else:
				sqls = [args.sql]
			query_start = datetime.now()
			query.query(sqls = sqls,description = args.description,std_out = args.std_out,csv = args.csv)
			query_finish = datetime.now()
			print "Query Time: ",query_finish - query_start
		else:
			rc.run()
	
	if args.population_query:
		
		from query import Filter,Population_Query
		from geopy.geocoders import Nominatim
		
		census = Census(target_dir,data_dir)
		census.set_dbs()
		
		pq_args = str(args.population_query)
		if re.search('[A-Za-z]',pq_args):
			geo = Nominatim()
			results = geo.geocode(pq_args)
			coordinates = (results.latitude,results.longitude)
		else:
			location = pq_args.split(',')
			coordinates = (float(location[0]),float(location[1]))
		
		assert(isinstance(coordinates,tuple))
		
		pq = Population_Query(census.census_db,census.census_table_name,coordinates = coordinates)
		
		search_hits = pq.query_tracts(float(args.radius))
		results = pq.get_population_dict()
		
		pq_variables = str(args.pq_variable).split(',')
		
		for variable in pq_variables:
			print variable,results[variable]
		
		print "But, there's more!"
		
		for variable in pq.demographic_variables():
			print variable,results[variable]
			
		if args.csv:
			pq.print_search_hits(args.csv,all_vars = True)
			
if __name__ == "__main__":
    main()
