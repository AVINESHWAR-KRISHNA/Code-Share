from pyspark.sql import SparkSession

# Define the SQL Server connection properties
server = "<server_name>"
database = "<database_name>"
table = "<table_name>"
username = "<username>"
password = "<password>"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
url = f"jdbc:sqlserver://{server};database={database};user={username};password={password};"

# Create a SparkSession
spark = SparkSession.builder.appName("CSV to SQL Server").getOrCreate()

# Read the CSV file as a Spark DataFrame
df = spark.read.csv("<path_to_csv_file>", header=True, inferSchema=True)

# Write the DataFrame to SQL Server
df.write.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).mode("append").save()

# Stop the SparkSession
spark.stop()
