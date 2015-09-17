from datetime import datetime
import os
import argparse



def main():
	sqlite_table = "path/to/sqlite_table.sqlite"
  table_name = "your table's  name"
	parser = argparse.ArgumentParser()
	parser.add_argument("--query","-q",dest = "query",action = "store_true")
	parser.add_argument("--queryfile","-qf",dest = "queryfile",default = "radio_census_query.txt")
	parser.add_argument("--sql",dest = "sql",default = "")
	parser.add_argument("--description","-dscr",dest = "description",action = "store_true")
	parser.add_argument("--std_out",dest = "std_out",action = "store_true")
	parser.add_argument("--csv",dest = "csv",default = "radio_census_results.txt")
	parser.add_argument("--d2","-d",dest = "d2",action = "store_true")
	parser.add_argument("--d2_variable","-ddv",dest = "dd_variable",default = "DP0010001")
	parser.add_argument("--percentile","-pct",dest = "percentile",default = 90)
	parser.add_argument("--population_query","-pq",dest = "population_query",default = None)
	parser.add_argument("--pq_variable","-pqv",dest="pq_variable",default = "DP0010001")
	parser.add_argument("--npr","-npr",dest="npr",action = "store_true")
	args = parser.parse_args()
	
	if args.query:
		print args.sql
		if args.sql:
			from query import Query
			query = Query(sqlite_table,table_name)
			if os.path.exists(args.sql):
				with open(args.sql) as f:
					sqls = f.read().split('\n')
			else:
				sqls = [args.sql]
			query_start = datetime.now()
			query.query(sqls = sqls,description = args.description,std_out = args.std_out,csv = args.csv)
			query_finish = datetime.now()
			print "Query Time: ",query_finish - query_start
	if args.population_query:
		
		rc = Radio_Census(target_dir,data_dir)
		rc.set_dbs()
		
		if os.path.exists(args.population_query):
			callsigns = []
			with open(args.population_query) as f:
				for line in f:
					callsigns.append(str(line.split('|')[1]).strip())
		else:
			callsigns = str(args.population_query).split(',')

		from query import Filter,Population_Query
		
		filter = Filter(callsigns,sqlite_table,table_name,'callsign')
		search_hits = filter.filter()
		filter.conn.close()
		census_table_name = ""
		census_sqlite_table = ""
		pq = Population_Query(search_hits,sqlite_table,table_name,census_sqlite_table,census_table_name)
		search_hits = pq.query_tracts()
		results = pq.get_population_dict()
		
		pq_variables = str(args.pq_variable).split(',')
		
		for variable in pq_variables:
			print variable,results[variable]
		
		print "But, there's more!"
		
		for variable in pq.demographic_variables():
			print variable,results[variable]
			
		if args.csv:
			pq.print_search_hits(args.csv)
