# Mingkang's funda_coding_assignment
The assignment is to conduct analysis on the dataset from a movie streaming service.

The analysis assignment was conducted on the Google Cloud Platform
The python code for ETL and data visualization is available in my Github

ETL and Preprocessing steps including:

- Extracting data from CSV files. 
- Parsing the release year information from movie titles, and converting it to datetime format. 
- Empty release year data was filled in with the date '1111'. 
- The genre information was separated into multiple rows and checked for misspellings. 
- 16 NaN tags were detected. 
- Timestamp text in the ratings and tags tables was transformed to datetime format.
- All tags were converted to lowercase. 
- The preprocessed tables were loaded into the BigQuery data warehouse."

Data visualization steps including:

- Creating views based on preprocessed data tables in the BigQuery data warehouse.
- Utilizing the Google Looker studio for visualization.
- As Looker studio has a maximum limit of 5000 points, I used matplotlib to visualize the data distribution for some of the exploratory data analysis.
