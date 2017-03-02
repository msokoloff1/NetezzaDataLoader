# NetezzaDataLoader

<img src="http://businessintelligence.com/wp-content/themes/bi/assets/images/vendor/netezza-logo.png" width="350px" height="95px"></img>
<p>
This program provides an open source solution to loading data into IBM's Netezza data warehousing application. 
</p>

####Current status:
The code has only been tested in a windows environment (though it should work in unix environments) and only offers support for loading from a mysql source. However, adding support for other source databases should be simple.

####To use:
1. Ensure that the "targetDriver" variable in the nzUtils.py and mysqlUtils.py point to the proper DSN on the machine running the program. 
2. Run the nzDataMigration.py file (python 3 required).
