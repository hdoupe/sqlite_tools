# sqlite_tools

Included is a sqlite_helper class that does everything from creating the sqlite table to adding rows of data to defining functions that I have found helpful in previous projects.  Please add functions that you find helpful or improving the ones that I created.  Also, the query.py file is essentially an interface between the user and the sqlite table.  Basically, it facilites writing sql commands and formatting data into csv files.  

Previously, I used these classes to pull data from a radio station and census sqlite table. That may explain some of the geography and demography oriented functions that I have defined.

Dependencies:
-	reverse_geocoder
https://github.com/thampiman/reverse-geocoder
-	haversine
https://github.com/mapado/haversine
-	geopy
https://github.com/geopy/geopy

Here are the flags and definitions:
	
	--std_out - print data in terminal
	--csv - print data in csv file (sep always '|')
	
	--query - execute query through Query interpreter
	--sql - statement executed by query interpreter
	--description - add description to csv file

	--population_query - get demographic totals for the the census tracts within a certain radius of either an adress or a pair of latitude/longitude coordinates
		python census.py --csv "path/to/your_file.csv" --population_query "309 Lee Street, Thomson, Georgia" --radius 10
		python census.py --std_out --population_query -33,82 --radius 50
	--pq_variable- select demographic totals for this variable.

	--percentile - percentile for condition
	--geocode – adds geocoded variables

Defined Functions:
	"REGEXP('callsign',callsign)" - Routine for matching callsigns
	"HAVERSINE(lat1,lon1,lat2,lon2)" - calculate distance between two points (km)
	"PCT(v1,v2)" - divides v1 by v2 and multiplies result by 100
	"MEAN(*args)" - calculate mean of given arguments
	"DIVERSITY_RACE(*demographic variables*)" - if you use query, then all one needs
		to do to call this function is to place '{div_race}' in one's SQL query, and
		Query will evaluate it
	"DIVERSITY_AGE(*demographic variables*)" This works the same as 'DIVERSITYRACE'.  
		Just replace '{div_race}' with '{div_age}'
	"PERCENTILE('variable',variable)" Returns the percentage of values under the queried number
	"GREATERTHANPERCENTILE('variable',variable,percentile)" Returns 1 if the given 
		value is greater than or equal to the value at the given percentile, 
		returns 0 otherwise
	"EDU(license)" Returns 1 if the license appears to be affiliated with an educational
		organization, returns 0 otherwise (see code for methodology)
	"POLITICALBOUNDARY('level',latitude,longitude)" This reverse geocodes the point and 
		returns the name of the city, state, or country when level is 'name','admin1'
		or 'cc'.


Examples of SQL commands:

SELECT callsign,trans_lat,trans_lon,IN_binary,NPR_binary,{div_age},{div_race}, PERCENTILE('DP0010001',DP0010001) FROM RC ORDER BY {div_age} DESC

SELECT callsign,trans_lat,trans_lon,POLITICALBOUNDARY('admin1',trans_lat,trans_lon) FROM RC WHERE IN_binary = 1 AND POLITICALBOUNDARY('admin1',trans_lat,trans_lon) = 'Georgia'

SELECT DP0010001,INTPTLAT10,INTPTLON10,{div_age},{div_race},PERCENTILE('DP0010001',DP0010001) FROM census  WHERE GREATERTHANPERCENTILE('DP0010001',DP0010001,90) = 1

Examples of Command-line usage:

python interpret.py --query --std_out --sql  “some sql command”

python interpret.py --query --csv "/path/to/output_file.csv" --sql  “some sql command”

Enjoy.  Let me know if you find bugs, etc...
