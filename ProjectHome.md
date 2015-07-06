# What's this do? #
> Import flat delimited files into a database MySql or MsSql on either linux or windows and auto optimize the sql table at the end.

# Reference #
  * [HowToRun](HowToRun.md) - example code to how to run the parser
  * [DestinationData](DestinationData.md) - what the vars do.

# Dependencies #
  * http://www.csvreader.com/java_csv.php - depends on open csvreader
  * http://commons.apache.org/lang/ - Apache Commons lang
  * http://dev.mysql.com/downloads/connector/j/5.1.html - MySql JDBC connector
  * http://msdn.microsoft.com/en-us/data/aa937724.aspx - Microsoft SQL JDBC connector

# Features #
> CSV 2 SQL, Tab Delimited to SQL, CSV2SQL, Tab2SQL

  * insert multiple files into one table
  * insert multiple files each having different fields
  * auto sql processes everything you may need
  * auto optimize the table columns sizes at the end
  * match source to new destination field/column names
  * Insert and Update depending on unique identities
  * Use more than one field/column for identity for match existing records.
  * insert or append to records.
  * auto create indexes
  * sample some or all your records for optimization
  * very easy to use for MySql

# TODOs #
  * Implemenet the auto indexing of identities so updates are faster. methods are there
  * deal with a delimited file with no field names in first record