# Stuff I want to remember #

# JDBC Heap Space Error On Resultset #
> When querying a large recordset with jdbc it loads the entire recordset into memory first. To overcome this you need to read row by row.

http://dev.mysql.com/doc/refman/5.4/en/connector-j-reference-implementation-notes.html
```
Statement select = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
// select.setFetchSize(1000); 
select.setFetchSize(Integer.MIN_VALUE);
ResultSet result = select.executeQuery(query);
```


# Querying a Boolean #
> Querying `Active`=[0|1], when you want both (`Active` IN (0,1))