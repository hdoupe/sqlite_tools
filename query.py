from datetime import datetime
from sqlite_helper import Sqlite_Helper
from haversine import haversine
import sqlite3 as sdb
import reverse_geocoder as rg
import re
import os
import numpy as np
import math

class Filter(Sqlite_Helper):
	def __init__(self,query,db,table_name,id_variable):
		super(Filter,self).__init__(db,table_name)
		self.query = query
		assert isinstance(self.query,list)

		self.conn,self.cursor = Sqlite_Helper.set_db(self)	

		self.table_name = table_name
		
		self.vars = self.get_vars()
		self.id_variable = id_variable
		
	def filter(self):
		misses = open('misses.txt','w')
		misses.write('misses')
		hit = 0
		miss = 0
		self.search_hits = {}
		search_misses = []
		sql1 = "SELECT " + ",".join(self.vars) + " FROM {tn} WHERE REGEXP('{id_value}',{id_variable})"
		sql2 = "SELECT " + ",".join(self.vars) + " FROM {tn} WHERE '{id_value}' = {id_variable}"
		sqls = [sql2,sql1]
		for id in self.query:
			
			results = self.search(sqls,self.id_variable,id)

			if results:
				id_value = results[self.id_variable]
				self.search_hits[id_value] = results
				print "HIT: ", id_value, id
				hit += 1
			else:
				print "MISS: ", id
				misses.write(id.strip() + '\n')
				search_misses.append(id)
		
		miss = len(set(search_misses))
		print hit
		print miss
		print hit + miss
		print float(hit) / (miss + hit)
		misses.close()
		return self.search_hits
	
	def search(self,sqls,vr, id_value):
		results = []
		for sql in sqls:
			self.cursor.execute(sql.\
				format(tn = self.table_name,id_value = id_value,id_variable = self.id_variable))
			temp = self.cursor.fetchall()
			if temp:
				results = temp[0]
				if len(temp) == 1:
					break
		return results
			
	
	def get_vars(self):
		self.cursor.execute("PRAGMA table_info({tn})".format(tn = self.table_name))
		vars = [dscr[1] for dscr in self.cursor.fetchall()]

		return vars
	

class Population_Query(Sqlite_Helper):
	def __init__(self,query, FCC_db,FCC_db_name, db, db_name = "",vars = []):	
		super(Population_Query,self).__init__(db,db_name)
		self.station_vector = self.get_station_vector(query,FCC_db,FCC_db_name)
		self.conn,self.cursor = Sqlite_Helper.set_db(self)
		self.search_hits = {}
		self.population_dict = {}
		self.db_name = db_name
		self.vars = Sqlite_Helper.demographic_variables(self)
		self.area_error = []
		self.radius_error = []
		self.census_tract_area = []
		self.correct_area = []
	
	def query_tracts(self):
		query_length = len(self.station_vector)
		
		for i,station in enumerate(self.station_vector):
			station_dictionary = self.station_vector[station]
			latitude = station_dictionary['trans_lat']
			longitude = station_dictionary['trans_lon']
			average_radius = station_dictionary['average reach']
			self.search_hits.update(self.search(latitude,longitude,average_radius))
			
			if i > 0 and i % 1000 == 0:
				print (float(i) / query_length) * 100
		
# 		calculate some statistics		
		print 'area',np.average(self.area_error)
		print 'radius',np.average(self.radius_error)
		print 'ct_area',np.sum(self.census_tract_area)
		print 'correct_area',np.sum(self.correct_area)
		radius_error = np.average(self.radius_error)
		ct_area = np.sum(self.census_tract_area)
		estimated_area = ct_area/(radius_error**2)
		print 'estimated_area',estimated_area
		
		return self.search_hits
	
	def search(self,latitude,longitude,average_radius):
		sql_string = ("SELECT * FROM {tn} "
			"WHERE HAVERSINE({latitude},{longitude},INTPTLAT10,INTPTLON10) <= {ar}")
		
		search_hits = {}
		area = 0.0
		
		while not search_hits:
			self.cursor.execute(sql_string.format(tn = self.db_name,latitude = latitude,
				longitude = longitude, ar = average_radius))
			results = self.cursor.fetchall()
			
			if results:
				for result in results:
					geo_id = result['GEOID10']
					search_hits[geo_id] = result
					area += result['ALAND10'] + result['AWATER10']
				
# 				calculate some area statistics
				area = area / 1000.0
				correct_radius = math.sqrt((area/math.pi))
				exact = math.pi * ((average_radius) ** 2)
				self.census_tract_area.append(area)
				self.correct_area.append(exact)
				self.radius_error.append(correct_radius/average_radius)
				self.area_error.append(abs(exact - area))
# 				print exact - area
			
			average_radius += 5
			if average_radius > 100:
				break

		return search_hits
			
	
	def set_population_dict(self,demographics = []):
		if not self.search_hits:
			return []
		
		if not demographics:
			demographics = Sqlite_Helper.demographic_variables(self)
		
		for demographic in demographics:
			total = 0
# 			iterate through tract tuples, add total population
			for tract in self.search_hits.itervalues():
				geo_id = tract[0]
				population = tract[demographic]
				try:
					total += population
				except TypeError as e:
					try:
						population = float(population)
					except ValueError as v:
						raise v
					total += population
			
			self.population_dict[demographic] = str(total)
		
		return self.population_dict
	
	def get_station_vector(self,query,FCC_db,FCC_db_name):
		SH = Sqlite_Helper(FCC_db,FCC_db_name)
		conn,cursor = SH.set_db()
		stations = {}
		
		for callsign in query:
			cursor.execute("SELECT * FROM {tn} WHERE callsign = '{callsign}'".\
				format(tn = FCC_db_name,callsign = callsign))
			results = cursor.fetchall()
			
			if results:
				results = results[0]
				type = results['type'].encode('ASCII')
				trans_lat = results['trans_lat']
				trans_lon = results['trans_lon']
				if type == 'FM':
					reach_lats,reach_lons = Sqlite_Helper.get_reach_vector(self,results)
					average_reach = Sqlite_Helper.get_average_reach(self,trans_lat,trans_lon,reach_lats,reach_lons)
				else:
					average_reach = 50
				stations[results['callsign'].encode('ASCII')] = {'trans_lat':trans_lat,'trans_lon':trans_lon,'average reach':average_reach}
		
		conn.close()
		
		return stations
	
	def get_vars(self,vars):
		
		if vars:
			if vars[0] != "GEOID10":
				vars.insert(0,"GEOID10")
			return vars
		
		else:
			self.cursor.execute("PRAGMA table_info({tn})".format(tn = self.table_name))
			vars = [dscr[1] for dscr in self.cursor.fetchall()]
			return vars
	
	def variables(self):
		return self.variables
			
	
	def get_population_dict(self, demographics = []):
		if self.population_dict:
			return self.population_dict
		
		else:
			if demographics:
				assert isinstance(demographics,list)
				self.set_population_dict(demographics = demographics)
			else:
				self.set_population_dict()
		
		return self.population_dict
	
	def get_results(self):
		assert self.search_hits
		if self.search_hits:
			return self.search_hits
	
	def print_search_hits(self,csv,sep = '|'):
		if csv:
			csv = open(csv,'w')
			csv.write('callsign' + sep + sep.join(self.vars) + '\n')

		for station in self.search_hits:
			row = self.search_hits[station]
			line = [station]
			for datum in station:
				line.append(str(datum))
			csv.write(sep.join(line) + '\n')


class Query(Sqlite_Helper):
	def __init__(self,RC_db, RC_table_name,secondary_db = "",secondary_table_name = ""):
		super(Query,self).__init__(RC_db,RC_table_name)

		self.conn, self.cursor = Sqlite_Helper.set_db(self)
		self.query_N = self.cursor.execute("SELECT count(*) FROM {tn}".format(tn = RC_table_name))
		
		self.results_path = ""
		self.description_path = ""

		self.secondary_db = secondary_db
		self.secondary_table_name = secondary_table_name
		if os.path.exists(self.secondary_db):
			self.allow_secondary_compare = True
		else:
			self.allow_secondary_compare = False
		
		self.secondary_cursor = None
		self.secondary_conn = None
			
	def query(self,sqls = [],description = False,compare = False,
		percent = False,sep = ' ',std_out = False,csv = ""):
		search_hits = []

		for sql in sqls:
			
			sql = self.evaluate_sql(sql)

			try:
			
				self.cursor.execute(sql)
			
			except Exception as e:
				import sys
				self.cursor.close()
				self.conn.close()
				print e
				sys.exit(0)

			results = self.cursor.fetchall()
			if results:
				variables = []
		
				for key in results[0].keys():
					variables.append(key)
				if csv:
					print_variables = self.extract_variables(variables)
				if std_out:
					rg.search((0,0))
					print sql
					print sep.join(variables)
					for rank,result in enumerate(results):
						print rank,
						for var in variables:
							print result[var],sep,
			
						if 'trans_lat' in variables and 'trans_lon' in variables:
							lat = result['trans_lat']
							lon = result['trans_lon']
							location = rg.search((lat,lon))[0]
							print location['name'],',',location['admin1'],sep
						elif 'INTPTLAT10' in variables and 'INTPTLON10' in variables:
							lat = result['INTPTLAT10']
							lon = result['INTPTLON10']
							location = rg.search((lat,lon))[0]
							print location['name'],',',location['admin1'],sep
						else:
							print ''

				if csv:
					self.print_csv(variables,results,csv)
				if description:
					self.description(variables,results,sql)
				if compare:
					self.compare_to_secondary(results,sql,percent = percent)
			
			search_hits += results

		return search_hits
	
# 	function replaces {div_race/age} with actual function name and variables
	def evaluate_sql(self,sql):
		patterns = {"div_race":re.compile('{div_race}'),"div_age":re.compile('{div_age}')}
		matches =[]
		for key in patterns:
			m = re.findall(patterns[key],sql)
			if m:
				matches.append(m[0])
		if matches:
			matches = set(matches)
			diversity = {}
			for match in matches:
				match = re.sub('{|}','',match)
				type = match.split('_')[1]
				diversity[type] = self.select_diversity(type = type)
			if 'age' in diversity and 'race' in diversity:
				sql = sql.format(div_race = diversity['race'],div_age = diversity['age'])
			elif 'age' in diversity:
				sql = sql.format(div_age = diversity['age'])
			elif 'race' in diversity:
				sql = sql.format(div_race = diversity['race'])
			else:
				pass
		return sql
	
	def select_diversity(self, type = "race"):
		if type == 'race':
			diversity = "DIVERSITY_RACE(DP0010001,DP0080003,DP0080004,DP0080005,DP0080006,DP0080014,DP0080019,DP0080020,DP0100002,DP0100007)"
		elif type == 'age':
			diversity = "DIVERSITY_AGE(DP0010001,DP0010002,DP0010003,DP0010004,DP0010005,DP0010006,DP0010007,DP0010008,DP0010009,DP0010010,DP0010011,DP0010012,DP0010013,DP0010014,DP0010015,DP0010016,DP0010017,DP0010018,DP0010019)"
		else:
			raise ValueError("Invalid type, must be either 'race' or 'age'")
		return diversity
	
	def print_csv(self,variables,results,csv = ""):
		if csv:
			path = csv
		else:
			path = input("enter path: " )
		csv = open(path,'w')
		csv.write("rank|" + '|'.join(variables) + '\n')
	
		for arb,result in enumerate(results):
			arb_plus_one = arb + 1
			csv.write(str(arb_plus_one) + '|')
			for i,num in enumerate(result):
				if i == len(result)-1:
					csv.write(str(num) + '\n')
				else:
					csv.write(str(num) + '|')
		csv.close()
	
	def demographic_variables(self,percent = False):
		p1 = re.compile("DP00[1-9]00[0-9][0-9]")
		p2 = re.compile("DP01[0|1]00[0-9][0-9]")
		re_patterns = [p1,p2]
		
		self.cursor.execute("PRAGMA table_info({tn})".format(tn = self.table_name))
		vars = [dscr[1] for dscr in self.cursor.fetchall()]
		
		variables = []
		
		for var in vars:
			for pattern in re_patterns:
				if re.search(pattern,var):
					variables.append(str(var))
					break

		if percent:
			pcts = []
			for variable in variables:
				pcts.append("PCT(" + variable + ",DP0010001)")
			return pcts
		else:
			return variables
	
	def get_args_from_file(self,args_file):
		with open(args_file) as f:
			args = f.read().split('\n')
		return args
	
	def extract_variables(self,keys):
		variables = []
		for key in keys:
			spl = re.split("[(|)|,]",key)
			if len(spl) == 4:
				variables.append(spl[1])
			else:
				variables.append(spl[0])
		return variables
