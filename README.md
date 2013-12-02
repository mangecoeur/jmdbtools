JMdbTools
---------

A simple cross platform command line tool to export MS Access .mdb files to CSV or MySQL.

It is largely a simple CLI wrapper around the [Jackcess](http://jackcess.sourceforge.net/) library which does all the
actual work.


Usage
-----

Run it using:

java -jar jmdbtools.jar -f {mdb file names} -options

Options are:

-s - show file stats
-e - export file
-db {database location} - export to MySQL database, eg. localhost/DBNAME
-u - database username
-p - database password
-o - set this flag to enable overwrite existing tables
-tp {prefix} - table prefix

