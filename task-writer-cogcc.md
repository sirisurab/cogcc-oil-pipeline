<project>cogcc</project>

<datasets>
	<dataset>
		<name>{year}_prod_reports.zip</name>
		<zip-file-path>https://ecmc.state.co.us/documents/data/downloads/production/{year}_prod_reports.zip</zip-file-path>
		<monthly-file-path>https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv</monthly-file-path>
		<data-dictionary>https://ecmc.state.co.us/documents/data/downloads/production/production_record_data_dictionary.htm</data-dictionary>
		<description>All oil and gas production from Colorado Energy & Carbon Management Commission (ECMC) for the specified year</description>
		<instructions>
			<instruction>Perform the following steps for each year from 2020 through current year
			 1. If year is current year, use the monthly-file-path to download csv file directly. Proceed to step 4.
			 2. Use the zip-file-path to download the zip file for the year
			 3. Unzip the zip file and extract the csv file
			 4. Place the resulting csv file in the `./data/raw` folder
			</instruction>
			<instruction>The data file is in .csv format. The data dictionary for the data file needs to be constructed from the html table at the page linked in the data-dictionary element. Use BeautifulSoup to parse the html table into `production-data-dictionary.csv` file and place it under `./references/`
			</instruction>
			<instruction>Rate-limit parallel downloads to a maximum of 5 concurrent workers via
			Dask. Add a 0.5 second sleep per download worker to avoid overloading the server.
			</instruction>
		</instructions>
	</dataset>
</datasets>

<data-filtering>
	<constraint>
	The ingest stage must filter rows to the target date range (year >= 2020) after reading raw files. Raw production files downloaded from ECMC may contain production history going back to the earlier years regardless of whether the acquire downloads data for specific years only. Extract year from ReportYear column
	</constraint>
</data-filtering>

<context>
The pipeline package is named `cogcc_pipeline`.
All test files must be under `tests/`.
Data files:
    - data/raw/{year}_prod_reports.csv  <- annual production CSVs
    - data/raw/monthly_prod.csv         <- current year monthly
Reference files in references/:
    - production-data-dictionary.csv
</context>
