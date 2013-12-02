JMdbTools
---------

A simple cross platform command line tool to export MS Access .mdb files to CSV or MySQL.

It is largely a simple CLI wrapper around the [Jackcess](http://jackcess.sourceforge.net/) library which does all the
actual work.


Usage
-----

You need Java 1.6+ , and you can run the jmdbtools.jar file directly.

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



Building
--------

JMdbTools is a Maven project. You can run it with the exec plugin using -Dargs to supply the required arguments, or you
can use the assembly plugin maven-assembly-plugin:single goal to generate the standalone jar package. 

