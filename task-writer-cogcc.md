<project>cogcc</project>

<pre-task-instructions>
	<instruction>Before writing any task spec, you must establish the exact column names from the
	raw data. Follow these steps in order:
	1. Use read_file to read `references/production-data-dictionary.csv` — this is the
	   authoritative source for column names, descriptions, dtypes, and nullability.
	2. Use fetch_url to retrieve the header row of the monthly CSV:
	   https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv
	   Read the first line only to confirm the exact column names as they appear in the raw files.
	3. Use these exact column names throughout all task specs.
	   Do not invent or guess column names — the data dictionary and CSV header are authoritative.
	</instruction>
</pre-task-instructions>

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

<data-dictionaries>
  <file>references/production-data-dictionary.csv</file>
  <description>Use production-data-dictionary.csv as the authoritative data-type source for all
  ingest, transform, and features task specs; data-type and nullable values must be
  mapped to pandas types per the Data dictionary ADR in ADRs.md
  </description>
</data-dictionaries>

<context>
The pipeline package is named `cogcc_pipeline`.
All test files must be under `tests/`.
Data files:
    - data/raw/{year}_prod_reports.csv  <- annual production CSVs
    - data/raw/monthly_prod.csv         <- current year monthly
Reference files in references/:
    - production-data-dictionary.csv
    - production-data-dictionary.md
</context>
