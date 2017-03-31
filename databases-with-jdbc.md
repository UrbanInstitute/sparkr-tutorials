# Database Connectivity - JDBC
Alex C. Engler, Urban Institute  

**Last Updated**: March 31, 2017


**Objective**: Understand the basic process of reading from a databse with SparkR.

* Read a .csv file into SparkR as a DF
* Measure dimensions of a DF
* Append a DF with additional rows

**SparkR Operations Discussed**: `read.jdbc`

***

:heavy_exclamation_mark: **Warning**: Before beginning this tutorial, please visit the SparkR Tutorials README file (found [here](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md)) in order to load the SparkR library and subsequently initiate a SparkR session.


The following error indicates that you have not initiated a SparkR session:


```r
Error in getSparkSession() : SparkSession not initialized
```

If you receive this message, return to the SparkR tutorials [README](https://github.com/UrbanInstitute/sparkr-tutorials/blob/master/README.md) for guidance.

***

### Interacting with databases using SparkR

Communicating with databases in Spark can be confusing, and the [official documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases) assumes a lot of knowledge on the subject. To streamline this tutorial, functionality for MySQL connectivity is already loaded in the bootstrap scripts associated with these tutorials

However, connecting with other database will require further alterations to the bootstrap scripts in order to do. The next step shows, as briefly as possible, how to incorporate additional connectors into Spark such that they will be available in SparkR. You can skip this step and move to Querying Databases with read.jdbc() if you want to work with MySQL.


#### Adding new database connectivity JAR files 

Currently, the bootstrap scripts for SparkR in the [Spark Social Science Repository](https://github.com/UrbanInstitute/spark-social-science/blob/master/sparkr/rstudio_sparkr_emr5lyr-proc.sh) install an add-on for working with mysql. This file is the MySQL Connector for JDBC, or [MySQL Coonector/J](https://dev.mysql.com/downloads/connector/j/5.1.html). In order for Spark to query MySQL databases (regardless of whether you are using SparkR, PySpark, or Scala), you need to install this connector, which is a .far file. To do this consistently, I first placed the zipped jar file in AWS S3. Then, [in the bootstrap scripts](https://github.com/UrbanInstitute/spark-social-science/blob/master/sparkr/rstudio_sparkr_emr5lyr-proc.sh#L256), I copy and unzip the zipped jar file to the /usr/lib/spark/jars folder, which Spark automatically loads.

```
aws s3 cp s3://ui-spark-social-science/emr-util/mysql-connector-java-5.1.41.tar.gz .
tar -xvzf mysql-connector-java-5.1.41.tar.gz
sudo mv mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar /usr/lib/spark/jars
rm -r mysql-connector-java-5.1.41
```

#### Querying databases with `read.jdbc()`

Once you have the appropriate jar file for your database of choice, you can use R's `read.jdbc()` function to query your database. Below, I am querying an AWS Aurora database (which is [AWS's MySQL-compatible database](https://aws.amazon.com/rds/aurora/)). 

First, you want to very carefully specify the URL for your database. You can see a full guide to the [JDBC URL Format](https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-configuration-properties.html) here.

```r
your_url <- "jdbc:mysql:database_location/database_name"

```

Then you an pass this url, as well as the table you would like to fetch, your username, and your password to `read.jdbc()`. I have replaced sensitive information below with pseudo code (all starting with your_), but as you can see this query returns a database table.

```r
dat <- read.jdbc(url=your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="your_table"
                 , user="your_username"
                 , password="your_password")
showDF(dat)
+----+--------+------------+------------+------------+-------------+------------+
|year|cbsa2013|      income|  coborrower|         fvr|household_num|     HO_rate|
+----+--------+------------+------------+------------+-------------+------------+
|2005|   10100|67.201716738| 0.624194348|0.1274169559|    16623.581|0.6901105724|
|2005|   10140|73.149889309|0.5887306502|0.0474303406|     27672.65|0.6843757284|
|2005|   10180|74.531343213|0.5631673751|0.1985347483|    61365.536|0.6299735734|
|2005|   10220|65.891544844|0.5482348499|0.0615442703|    13952.516|0.7034758462|
|2005|   10300|66.492471861|0.5270212463|0.0242406959|    36797.835|0.8058110484|
|2005|   10420|72.387847984| 0.450191753|  0.04583056|     275804.0|0.7121252774|
|2005|   10460|65.972204473|0.5640243902|0.1585365854|    23544.694|0.6620042715|
|2005|   10500|81.374914636|0.3969955829|0.0890417475|    58925.175|0.5531117218|
|2005|   10540|75.104313725|0.5797137216|0.0431885489|    42981.884|0.6139988187|
|2005|   10580|81.350642885|0.4866869583|0.0519139072|   339203.657|0.6682141343|
|2005|   10620|69.198223658|0.4972972973|0.0416988417|     22368.15|0.7293573675|
|2005|   10660|63.518017121|0.5485965247|0.0700782891|    11995.206|0.7867968253|
|2005|   10700|       68.99|0.4688156973|0.0613174492|     34730.18|0.7192600211|
|2005|   10740|  84.0370695|0.4716269473|0.0885541487|    321252.45|0.6785414399|
|2005|   10780|72.730282168|0.4906179416|0.0688487227|    56458.385|0.6875603154|
|2005|   10820|69.154183813|0.5622710623|0.0486656201|    14777.162|0.8136202337|
|2005|   10860|97.274583333| 0.548438751|0.0768614892|    15201.276|0.7425035898|
|2005|   10900|81.816108621|0.5342383295|0.0285613467|     301885.0|0.7342796098|
|2005|   10940|62.740952381|0.5138953219|0.0750347383|    15214.992|0.7151472705|
|2005|   10980| 76.66849978|0.5423190859|0.0387219636|    12815.152|0.8506332972|
+----+--------+------------+------------+------------+-------------+------------+
only showing top 20 rows
```

This is a fairly intuitive way to query an entire table, assuming that you have done some form of ODBC or JDBC in the past. However, for more complex queries, the syntax get less obvious.


#### Where statements within the `predicates` argument

If you haven't used a ton of SQL, you may not be familiar with the use of the word [predicates](http://www.dummies.com/programming/databases/sql-where-clause-predicates-2/). in SQL-speak, predicates simply imply row-wise filtering of a table. So, anything you would do with a `WHERE` statement in SQL is included in `read.jdbc`'s `predicates` argument. 

Below, I include a simple row-wise filtering criteria, asking for the same table as before, except only the rows of data in which the `income` column is over 75.

```r
dat <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="your_table"
                 , predicates=list("income > 75")
                 , user="your_username"
                 , password="your_password")
showDF(dat)
+----+--------+------------+------------+------------+-------------+------------+
|year|cbsa2013|      income|  coborrower|         fvr|household_num|     HO_rate|
+----+--------+------------+------------+------------+-------------+------------+
|2005|   10500|81.374914636|0.3969955829|0.0890417475|    58925.175|0.5531117218|
|2005|   10540|75.104313725|0.5797137216|0.0431885489|    42981.884|0.6139988187|
|2005|   10580|81.350642885|0.4866869583|0.0519139072|   339203.657|0.6682141343|
|2005|   10740|  84.0370695|0.4716269473|0.0885541487|    321252.45|0.6785414399|
|2005|   10860|97.274583333| 0.548438751|0.0768614892|    15201.276|0.7425035898|
|2005|   10900|81.816108621|0.5342383295|0.0285613467|     301885.0|0.7342796098|
|2005|   10980| 76.66849978|0.5423190859|0.0387219636|    12815.152|0.8506332972|
|2005|   11100|78.848508013|0.5540630028| 0.147394527|    89258.968|0.6782921801|
|2005|   11180|76.478492421|0.5952780286|0.0300606373|    29262.192|0.6634777053|
|2005|   11260|92.470174809|0.5149273424|0.1709462189|   128671.688|0.6342618588|
|2005|   11380|80.347020934|0.5534883721| 0.107751938|     4933.478|0.7530488633|
|2005|   11460|99.644085622|0.4780476275|0.0170182592|     130765.0|0.6300233243|
|2005|   11660| 78.62245104|0.5715630885|0.0559322034|     8848.162|0.6880647077|
|2005|   11700|87.539269667| 0.495847426|0.0129103632|   166178.359|0.7101060734|
|2005|   11820|98.090178571|0.5875132275|0.0402116402|    14930.255|0.6907748729|
|2005|   11980|98.647760232|0.5453144266|0.1045006165|    27171.087|0.7295177039|
|2005|   12020|93.055572613|0.4219230758|0.0449829624|    67780.654|0.5464337656|
|2005|   12060|89.449260407|0.3212959528|0.0612416282|  1788687.494|0.6727994695|
|2005|   12100|103.80199603|0.4506616063|0.0298492153|     102471.0|0.6816953089|
|2005|   12180|76.535860656|0.4996677741|0.0800664452|    31006.409|0.7556315212|
+----+--------+------------+------------+------------+-------------+------------+
only showing top 20 rows
```

Don't be confused by the passing of a list() to the `predicates` argument: if you want to use multiple WHERE clause predicates, you simply include them in the same string connected by [normal MySQL syntax](https://www.techonthenet.com/mysql/and_or.php). 


```r
dat <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="your_table"
                 , predicates=list("income > 75 and year in (2006,2007,2008)")
                 , user="your_username"
                 , password="your_password")
showDF(dat)
+----+--------+------------+------------+------------+-------------+------------+
|year|cbsa2013|      income|  coborrower|         fvr|household_num|     HO_rate|
+----+--------+------------+------------+------------+-------------+------------+
|2006|   10140|81.539676474|0.5628208262|0.0435101442|     28241.54|0.6979506075|
|2006|   10180|79.425502893|0.5246499751|0.1815172144|    61038.424|0.6463764202|
|2006|   10420|77.907884875|0.4354786488|0.0555095874|     280837.0|0.6984549757|
|2006|   10500|76.968667714|0.3994421098|0.1014795873|    59771.085|0.5555275599|
|2006|   10540|82.328751909|0.5676318921|0.0415179162|    43429.059|0.6319768292|
|2006|   10580|85.049615361|0.4567932632|0.0564300535|    337961.36|0.6478584179|
|2006|   10620|77.098087098|0.4937402191|0.0348200313|    19948.425|0.7825617812|
|2006|   10740|88.048909729|0.4597368247|0.0683223305|     318660.4|0.6841711427|
|2006|   10860|103.32390074|0.5183776933| 0.075200676|     13719.54|0.7118390267|
|2006|   10900|86.501705619|0.5048979502|0.0287022091|     305792.0| 0.736402522|
|2006|   11100|80.658281707|0.5288016321|0.1145373678|    87277.552|0.6496221617|
|2006|   11180|80.377880832|0.5712760278|0.0319006941|    31343.844|0.6396942251|
|2006|   11260|101.61763674|0.4906544626|0.1471117566|    127674.52|0.6463121381|
|2006|   11380|77.542530379|0.5210084034|0.0840336134|     4889.526|0.7317547754|
|2006|   11460|105.51947439|0.4600588368|0.0173493249|     134053.0|0.6279978814|
|2006|   11540|75.146105191|0.6167615503| 0.052277206|     87734.65|0.7088581877|
|2006|   11580|79.149761428|0.4143501674|0.0467180905|    12329.157|0.7940348233|
|2006|   11660|81.250834725|0.5291109363|0.0542879622|       8974.4|0.7203797468|
|2006|   11700|99.575200728|0.4829232361|0.0198791066|   164669.603|0.7203291369|
|2006|   11820|110.37491099|0.5496191756|0.0316980287|     15423.52|0.6872105071|
+----+--------+------------+------------+------------+-------------+------------+
only showing top 20 rows
```
Similarly, we can use and and or together:

```r
dat <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="your_table"
                 , predicates=list("(income < 70 and year = 2005) or
                                    (income > 100 and year in (2006,2007))")                 
                 , user="your_username"
                 , password="your_password")

## Averages by year show the subset worked:
dat_agg <- agg(groupBy(dat, dat$year)
               ,avg_income = mean(dat$income))

showDF(dat_agg)
```

#### Selecting columns within the `tablename` argument 

Spark requires an odd syntax to perform a query that uses a MySQL `SELECT` statement (that is, columnar sub-selection from a single table). Within the `tableName` argument, we need to write our `SELECT` statement, wrapped in parenthesis as though to create a temporary table (called `tmp` below). Below, I use `SELECT` to query only the `year` and `income` columns from the table.

```r
dat2 <- read.jdbc(url= your_url
                 , source="jdbc"
                 , driver="com.mysql.jdbc.Driver"
                 , tableName="(SELECT year, income FROM your_table) AS tmp"
                 , user="your_username"
                 , password="your_password")
showDF(dat2)
```







__End of tutorial__


