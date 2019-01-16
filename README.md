Requirements :
--------------

* Python 2.7.15rc1 or above but not 3

* pip

* python postgres driver : psycopg2 version 2.7.6.1 or above   (pip install psycopg2)

* python json module: json 2.0.9 or above (pip install json)

* python mongodb driver: pymongo 3.5.1 or above 

* Postgres (Version 11)

* Database: Mongodb version 4.0 or above (https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)


Steps:
------

* Modify the configuration file (config.json) according to your needs.

* Run pg2mongo_migration.py using command: python pg2mongo_migration.py


When to migrate from postgres to mongodb:
----------------------------------------

* If you are still in the initial stages of a project and something tells you that the tables you created need not have relationships.
* The external sources you use to populate your tables in postgres changes (There is no consistency in the data that you get.)
* You are unfamiliar with postgres (:D).
* You are working in a company where your Project Manager lets you play with different databases to check its analytical capabilities, scaling, availability and other stuffs.
* And many more.

Finally :
---------

* Unless and untill you are absolutely sure that you know what you are doing, only then you need to migrate from postgres to mongodb.
* My piece of advice: Dont migrate from postgres to mongodb **********************
