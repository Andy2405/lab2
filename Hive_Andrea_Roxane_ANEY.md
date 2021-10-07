1 Hive Beeline Client
In this tutorial, we will verify the connection to Hive using Beeline client.

1.1 Create a Connection to Beeline Client

	* Let's connect to HADOOP cluster using SSH.

Using username "a.aney".
Keyboard-interactive authentication prompts from server:
End of keyboard-interactive prompts from server
Access denied
Keyboard-interactive authentication prompts from server:
| Password:
End of keyboard-interactive prompts from server
Last failed login: Wed Sep 29 14:15:47 CEST 2021 from nat-in-drelab.groupe-efrei.fr on ssh:notty
There were 5 failed login attempts since the last successful login.
Last login: Tue Sep 21 23:55:45 2021 from 185.230.76.132


	*Let's initialize a Kerberos ticket.

[a.aney@hadoop-edge01 ~]$ kinit
Password for a.aney@EFREI.ONLINE:

	*Let's type the command beeline in the terminal prompt, when prompted press
enter as username and as password.

[a.aney@hadoop-edge01 ~]$ beeline
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/odp/1.0.3.0-223/hive/lib/log4j-slf4j-impl-2.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/odp/1.0.3.0-223/hadoop/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Connecting to jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/default;httpPath=cliservice;principal=hive/_HOST@EFREI.ONLINE;serviceDiscoveryMode=zooKeeper;ssl=true;transportMode=http;zooKeeperNamespace=hiveserver2
21/09/29 14:20:49 [main]: INFO jdbc.HiveConnection: Connected to hadoop-master03.efrei.online:10001
Connected to: Apache Hive (version 3.1.2.1.0.3.0-223)
Driver: Hive JDBC (version 3.1.2.1.0.3.0-223)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 3.1.2.1.0.3.0-223 by Apache Hive

	*Let's connect to the Hive using the command !connect:

jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.
online:2181,hadoop-master03.efrei.online:2181/;serviceDiscoveryMode=
zooKeeper;zooKeeperNamespace=hiveserver2

0: jdbc:hive2://hadoop-master01.efrei.online:> !connect jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
Connecting to jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
Enter username for jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/: a.aney
Enter password for jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/: ********
21/09/29 14:33:41 [main]: INFO jdbc.HiveConnection: Connected to hadoop-master03.efrei.online:10001
Connected to: Apache Hive (version 3.1.2.1.0.3.0-223)
Driver: Hive JDBC (version 3.1.2.1.0.3.0-223)
Transaction isolation: TRANSACTION_REPEATABLE_READ

	*Let's type help command for list of beeline commands.
1: jdbc:hive2://hadoop-master01.efrei.online:> help
!addlocaldriverjar  Add driver jar file in the beeline client side.
!addlocaldrivername Add driver name that needs to be supported in the beeline
                    client side.
!all                Execute the specified SQL against all the current connections
!autocommit         Set autocommit mode on or off
!batch              Start or execute a batch of statements
!brief              Set verbose mode off
!call               Execute a callable statement
!close              Close the current connection to the database
!closeall           Close all current open connections
!columns            List all the columns for the specified table
!commit             Commit the current transaction (if autocommit is off)
!connect            Open a new connection to the database.
!dbinfo             Give metadata information about the database
!delimiter          Sets the query delimiter, defaults to ;
!describe           Describe a table
!dropall            Drop all tables in the current database
!exportedkeys       List all the exported keys for the specified table
!go                 Select the current connection
!help               Print a summary of command usage
!history            Display the command history
!importedkeys       List all the imported keys for the specified table
!indexes            List all the indexes for the specified table
!isolation          Set the transaction isolation for this connection
!list               List the current connections
!manual             Display the BeeLine manual
!metadata           Obtain metadata information
!nativesql          Show the native SQL for the specified statement
!nullemptystring    Set to true to get historic behavior of printing null as
                    empty string. Default is false.
!outputformat       Set the output format for displaying results
                    (table,vertical,csv2,dsv,tsv2,xmlattrs,xmlelements,and
                    deprecated formats(csv, tsv))
!primarykeys        List all the primary keys for the specified table
!procedures         List all the procedures
!properties         Connect to the database specified in the properties file(s)
!quit               Exits the program
!reconnect          Reconnect to the database
!record             Record all output to the specified file
!rehash             Fetch table and column names for command completion
!rollback           Roll back the current transaction (if autocommit is off)
!run                Run a script from the specified file
!save               Save the current variabes and aliases
!scan               Scan for installed JDBC drivers
!script             Start saving a script to a file
!set                Set a beeline variable
!sh                 Execute a shell command
!sql                Execute a SQL command
!tables             List all the tables in the database
!typeinfo           Display the type map for the current connection
!verbose            Set verbose mode on
Comments, bug reports, and patches go to ???

 	*Which command allows you to view the jdbc connection used to connect to HiveServer2?

0: jdbc:hive2://hadoop-master01.efrei.online:> !list
1 active connection:
 #0  open     jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2


 	* Let's list all databases.

1: jdbc:hive2://hadoop-master01.efrei.online:> show databases;
INFO  : Compiling command(queryId=hive_20211006234801_65efd186-15ef-4002-bf1-7e952033a212): show databases
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:databas_name, type:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006234801_65efd186-15e-4002-bf1f-7e952033a212); Time taken: 0.055 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006234801_65efd186-15ef-4002-bf1-7e952033a212): show databases
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006234801_65efd186-15e-4002-bf1f-7e952033a212); Time taken: 0.012 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+----------------------+
|    database_name     |
+----------------------+
| a_chevron            |
| a_ngau               |
| a_poloubinski        |
| a_tonlop             |
| adrouineau_areino    |
| aflorentz            |
| ajond                |
| alemasne             |
| amessika             |
| armel                |
| arouhi               |
| arthy_kandiah        |
| avellard             |
| b_gabrieleff         |
| balthazar_r          |
| baudouindlv          |
| bcimetiere           |
| bclaudic             |
| bcourtois            |
| benjamin             |
| berenice             |
| bfoin                |
| cedric_kabore        |
| cgrandclaude         |
| cjdidi               |
| cmauvezin            |
| cvenet               |
| default              |
| droguet              |
| e_bernard            |
| e_diez               |
| e_guedj              |
| e_joliman            |
| e_yimene_kaze        |
| ebitton              |
| emp                  |
| eya_database         |
| eya_gheyouche        |
| f_montiel            |
| godard               |
| graiess              |
| gudronlauret         |
| gugulpb              |
| h_feruglio           |
| halvarez             |
| hdubouillon          |
| hive1                |
| hugokauffman         |
| hugues_roland        |
| i_aderdour           |
| i_jelila             |
| ilyass_bourkaik      |
| imeira               |
| ines_benabed         |
| information_schema   |
| j_ditsouga_perera    |
| jbenayad             |
| jcharbonnaud         |
| jcrecel              |
| jehl                 |
| jingtao_qu           |
| kgwet                |
| ktan                 |
| ktazi                |
| l_barazer            |
| l_etzol              |
| l_ferrand            |
| l_maiz               |
| lanthoine            |
| lgaillard            |
| llecomte             |
| lrodriguez           |
| lucas_maiz           |
| m_abatti             |
| m_iloo_liandja       |
| ma_baseismail        |
| maboualam            |
| mame                 |
| mame_aicha_dieye     |
| marieblein           |
| mbourabah            |
| mdufau               |
| melyachkouri         |
| mhanania             |
| mhatoum              |
| moukaci              |
| ncailleux            |
| ncolard              |
| ninon                |
| nourris              |
| nruiz                |
| ntharmaseelan        |
| nzebair              |
| p_huang              |
| p_ngoufack           |
| pbertrand            |
| pho                  |
| pmeignan             |
| q_nicot              |
| r_hetet              |
+----------------------+
|    database_name     |
+----------------------+
| r_ouedraogo          |
| rania_elha           |
| rbocchini            |
| rbouderghouma        |
| rbourourou           |
| revillon             |
| rhetet               |
| rlegrand             |
| rleveille_nizerolle  |
| s_ano                |
| s_touret             |
| sarnaud              |
| sbrisard             |
| schen                |
| sghlouci             |
| spiquet              |
| spruvot              |
| svassent             |
| sys                  |
| t_goujon             |
| temp                 |
| vaihau_williamu      |
| vanelle_domo         |
| vbonneff             |
| vincent_ly           |
| vincent_tran         |
| wang_ye              |
| wangye               |
| yang_chen            |
| yingda               |
| ymaassouli           |
+----------------------+
131 rows selected (0.173 seconds)


 	* If not exists, let's create a database using your username.

1: jdbc:hive2://hadoop-master01.efrei.online:> create database a_aney;
INFO  : Compiling command(queryId=hive_20211006235024_325bbeb8-fc52-473c-926-cf10c3126d93): create database a_aney
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235024_325bbeb8-fc5-473c-9269-cf10c3126d93); Time taken: 0.015 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235024_325bbeb8-fc52-473c-926-cf10c3126d93): create database a_aney
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235024_325bbeb8-fc5-473c-9269-cf10c3126d93); Time taken: 0.044 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.076 seconds)

NOTE: I checked and my table was actually created

1: jdbc:hive2://hadoop-master01.efrei.online:> show databases;
INFO  : Compiling command(queryId=hive_20211006235057_8a2a034b-b37b-4dc5-96e-af2e4e79a849): show databases
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:databas_name, type:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235057_8a2a034b-b37-4dc5-96eb-af2e4e79a849); Time taken: 0.011 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235057_8a2a034b-b37b-4dc5-96e-af2e4e79a849): show databases
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235057_8a2a034b-b37-4dc5-96eb-af2e4e79a849); Time taken: 0.01 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+----------------------+
|    database_name     |
+----------------------+
| a_aney               |


	*Let's use my database a_aney

1: jdbc:hive2://hadoop-master01.efrei.online:> use a_aney;
INFO  : Compiling command(queryId=hive_20211006235158_7ee9ff9b-1519-445c-90d3                                                                                   -69b7b02c4ca1): use a_aney
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235158_7ee9ff9b-1519                                                                                   -445c-90d3-69b7b02c4ca1); Time taken: 0.039 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235158_7ee9ff9b-1519-445c-90d3                                                                                   -69b7b02c4ca1): use a_aney
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235158_7ee9ff9b-1519                                                                                   -445c-90d3-69b7b02c4ca1); Time taken: 0.037 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.094 seconds)


 	*Let's list the tables.

1: jdbc:hive2://hadoop-master01.efrei.online:> show tables;
INFO  : Compiling command(queryId=hive_20211006235229_ab96124b-6a03-407d-ab7-98d606edc69b): show tables
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:tab_nam, type:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235229_ab96124b-6a0-407d-ab7d-98d606edc69b); Time taken: 0.022 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235229_ab96124b-6a03-407d-ab7-98d606edc69b): show tables
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235229_ab96124b-6a0-407d-ab7d-98d606edc69b); Time taken: 0.011 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------+
| tab_name  |
+-----------+
+-----------+
No rows selected (0.051 seconds)


 	* Let's create table called temp with a column called col of String type.

1: jdbc:hive2://hadoop-master01.efrei.online:> create table temp(col String)
INFO  : Compiling command(queryId=hive_20211006235311_57d4be18-8130-4b9a-9e2-6de4101e780f): create table temp(col String)
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235311_57d4be18-813-4b9a-9e20-6de4101e780f); Time taken: 0.033 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235311_57d4be18-8130-4b9a-9e2-6de4101e780f): create table temp(col String)
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235311_57d4be18-813-4b9a-9e20-6de4101e780f); Time taken: 0.093 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.143 seconds)

 	
	*Let's confirm the table creation.

1: jdbc:hive2://hadoop-master01.efrei.online:> show tables;
INFO  : Compiling command(queryId=hive_20211006235402_10648e42-46b0-47b2-bd2-4c0a12e894b2): show tables
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:tab_nam, type:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235402_10648e42-46b-47b2-bd2c-4c0a12e894b2); Time taken: 0.022 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235402_10648e42-46b0-47b2-bd2-4c0a12e894b2): show tables
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235402_10648e42-46b-47b2-bd2c-4c0a12e894b2); Time taken: 0.01 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------+
| tab_name  |
+-----------+
| temp      |
+-----------+
1 row selected (0.062 seconds)
NOTE: It is clear that the "temp" table has been created.
1: jdbc:hive2://hadoop-master01.efrei.online:> show columns from temp
. . . . . . . . . . . . . . . . . . . . . . .> ;
INFO  : Compiling command(queryId=hive_20211006235449_209beb12-c342-47a5-8ed-b795f00e3eb7): show columns from temp
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:field, ype:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235449_209beb12-c34-47a5-8edb-b795f00e3eb7); Time taken: 0.051 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235449_209beb12-c342-47a5-8ed-b795f00e3eb7): show columns from temp
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235449_209beb12-c34-47a5-8edb-b795f00e3eb7); Time taken: 0.038 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+--------+
| field  |
+--------+
| col    |
+--------+
1 row selected (0.128 seconds)
NOTE: The column was also successfully created.

 	*Let's list the columns (name, data type, etc) of temp table.

1: jdbc:hive2://hadoop-master01.efrei.online:> describe temp col;
INFO  : Compiling command(queryId=hive_20211006235607_856c72f1-d54c-4bce-9975-45cf904cc44e): describe temp col
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:col_name, type:string, comment:from deserializer), FieldSchema(name:data_type, type:string, comment:from deserializer), FieldSchema(name:comment, type:string, comment:from deserializer)], properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235607_856c72f1-d54c-4bce-9975-45cf904cc44e); Time taken: 0.054 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235607_856c72f1-d54c-4bce-9975-45cf904cc44e): describe temp col
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235607_856c72f1-d54c-4bce-9975-45cf904cc44e); Time taken: 0.037 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+------------------------+----------------------------------------------------+--------------------+
|        col_name        |                     data_type                      |      comment       |
+------------------------+----------------------------------------------------+--------------------+
| col                    | string                                             | from deserializer  |
| COLUMN_STATS_ACCURATE  | {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"col\":\"true\"}} | NULL               |
+------------------------+----------------------------------------------------+--------------------+
2 rows selected (0.131 seconds)


	*Let's remove the table.

1: jdbc:hive2://hadoop-master01.efrei.online:> Drop table temp;
INFO  : Compiling command(queryId=hive_20211006235706_93c704be-3cf9-47dc-b191-665ff793a85b): Drop table temp
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hive_20211006235706_93c704be-3cf9-47dc-b191-665ff793a85b); Time taken: 0.054 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hive_20211006235706_93c704be-3cf9-47dc-b191-665ff793a85b): Drop table temp
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hive_20211006235706_93c704be-3cf9-47dc-b191-665ff793a85b); Time taken: 0.193 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.262 seconds)

NOTE: As we can see below, the temp table has been removed
1: jdbc:hive2://hadoop-master01.efrei.online:> show tables;
...
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------+
| tab_name  |
+-----------+
+-----------+
No rows selected (0.081 seconds)


	*Let's type !quit to exit the beeline shell
1: jdbc:hive2://hadoop-master01.efrei.online:> !quit
Closing: 1: jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
Closing: 0: jdbc:hive2://hadoop-master01.efrei.online:2181,hadoop-master02.efrei.online:2181,hadoop-master03.efrei.online:2181/default;httpPath=cliservice;principal=hive/_HOST@EFREI.ONLINE;serviceDiscoveryMode=zooKeeper;ssl=true;transportMode=http;zooKeeperNamespace=hiveserver2
[a.aney@hadoop-edge01 ~]$


1.2 Create tables

We are going to write some SQL Hive queries on the remarkable trees of Paris.

	*Let's create an external table called trees_external.
NOTE: I will put the dataset to download in my home directory
[a.aney@hadoop-edge01 ~]$ cat > dataset
GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV
(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars
...
(48.8302532096, 2.41400587444);12;Acer;opalus;Sapindaceae;1870;15.0;160.0;Ile de Bercy;Erable d'Italie;;91;Bois de Vincennes (Ile de Bercy)
...

NOTE: I will put this file in the cluster (my hdfs environment)

[a.aney@hadoop-edge01 ~]$ hdfs dfs -put dataset dataset
[a.aney@hadoop-edge01 ~]$ hdfs dfs -ls

-rw-r--r--   3 a.aney a.aney      16680 2021-10-07 18:29 dataset

NOTE: Je compte utiliser ma base de données (a_aney)
[a.aney@hadoop-edge01 ~]$ beeline
0: jdbc:hive2://hadoop-master01.efrei.online:> use a_aney
. . . . . . . . . . . . . . . . . . . . . . .> ;
INFO  : Compiling command(queryId=hive_20211007183133_aa947fba-7524-4dd1-a73

0: jdbc:hive2://hadoop-master01.efrei.online:>  CREATE EXTERNAL TABLE trees_external(GEOPOINT string, ARRONDISSEMENT int, GENRE string, 
ESPECE string, FAMILLE string, ANNEEPLANTATION int, HAUTEUR float, CIRCONFERENCE float, ADRESSE string,
NOMCOMMUN string, VARIETE string, OBJECTID int, NOM_EV string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'  TBLPROPERTIES ("skip.header.line.count"="1");
INFO  : Compiling command(queryId=hive_20211007183627_3af53ab1-a7ca-4705-827b-ffb56d736b5a): CREATE EXTERNAL TABLE trees_external(GEOPOINT string, ARRONDISSEMENT int, GENRE string, 
ESPECE string, FAMILLE string, ANNEEPLANTATION int, HAUTEUR float, CIRCONFERENCE float, ADRESSE string, NOMCOMMUN string, VARIETE string, OBJECTID int, NOM_EV string)                                                                          
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'  TBLPROPERTIES ( "skip.header.line.count"="1")
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.077 seconds)

	*Let's create an internal table called trees_internal.

0: jdbc:hive2://hadoop-master01.efrei.online:> CREATE TABLE trees_internal(GEOPOINT string, ARRONDISSEMENT int, GENRE string, ESPECE string, 
FAMILLE string, ANNEEPLANTATION int, HAUTEUR float, CIRCONFERENCE float, ADRESSE string, NOMCOMMUN string, VARIETE string, OBJECTID int, NOM_EV string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY ' \n'  TBLPROPERTIES ("skip.header.line.count"="1");
INFO  : Compiling command
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.218 seconds)


	*Let's import data to the internal table using the external table.

0: jdbc:hive2://hadoop-master01.efrei.online:> load data inpath "/user/a.aney/dataset" overwrite into table trees_external;
INFO  : Compiling command(queryId=hive_20211007184207_c5eaf141-856f-4962-a6ba-0da668d116b8): load data inpath "/user/a.aney/dataset" overwrite into table trees_external
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
No rows affected (0.472 seconds)



	*Let's verify that each table got the same lines count.

NOTE: Let’s first check that the tables exist
0: jdbc:hive2://hadoop-master01.efrei.online:> show tables;
INFO  : Compiling command(queryId=hive_20211007184536_8565ace7-bd8c-48a0-b0d9-0f6cda2fe966): show tables
...
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+
|    tab_name     |
+-----------------+
| trees_external  |
| trees_internal  |
+-----------------+
2 rows selected (0.084 seconds)
NOTE: The tables have been created

0: jdbc:hive2://hadoop-master01.efrei.online:> select * from trees_external
. . . . . . . . . . . . . . . . . . . . . . .> ;
...
------------------+--------------------------+----------------------------------------------------+
|     trees_external.geopoint     | trees_external.arrondissement  | trees_external.genre  | trees_external.espece  | trees_external.famille  | trees_external.annee_plant  | trees_external.hauteur  | trees_external.circonference  |              trees_external.addresse               | trees_external.nom_comune  | trees_external.variete  | trees_external.objetcid  |               trees_external.nom_ev                |
+---------------------------------+--------------------------------+-----------------------+------------------------+-------------------------+-----------------------------+-------------------------+-------------------------------+----------------------------------------------------+----------------------------+-------------------------+--------------------------+----------------------------------------------------+
...
98 rows selected (0.734 seconds)

NOTE: Let’s check now for trees_internal

0: jdbc:hive2://hadoop-master01.efrei.online:> select * from trees_internal;
INFO  : Compiling command(queryId=hive_20211007202749_35012fa6-783b-4a0b-931b-b67b61d936b9): select * from trees_internal
...
------------------+--------------------------+----------------------------------------------------+
|     trees_internal.geopoint     | trees_internal.arrondissement  | trees_internal.genre  | trees_internal.espece  | trees_internal.famille  | trees_internal.annee_plant  | trees_internal.hauteur  | trees_internal.circonference  |              trees_internal.addresse               | trees_internal.nom_comune  | trees_internal.variete  | trees_internal.objetcid  |               trees_internal.nom_ev                |
+---------------------------------+--------------------------------+-----------------------+------------------------+-------------------------+-----------------
...
98 rows selected (0.887 seconds)

NOTE: After checking, each table has the same number of rows

1.3 Create queries

In this part, we will perform the same queries as in MapReduce using the internal table created previously. 
	*We will create queries that:

NOTE:Before typing the commands and to avoid errors, I used the following command:

0: jdbc:hive2://hadoop-master01.efrei.online:> set hive.execution.engine=mr;
No rows affected (0.01 seconds)


		*displays the list of distinct containing trees:

NOTE:I think that in this sentence there is a word missing but not to skip this question,
we will display the list of circumferences and also rounding.

			*Circonference
0: jdbc:hive2://hadoop-master01.efrei.online:> select distinct circonference from trees_internal
. . . . . . . . . . . . . . . . . . . . . . .> ;
INFO  : Compiling command(queryId=hive_20211007222316_13b4be2d-ee70-474c-b64b-e39b5f76892b): select distinct circonference from trees_internal
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+----------------+
| circonference  |
+----------------+
| 50.0           |
| 72.0           |
| 120.0          |
| 140.0          |
| 145.0          |
| 160.0          |
| 162.0          |
| 180.0          |
| 190.0          |
| 195.0          |
| 200.0          |
| 205.0          |
| 210.0          |
| 215.0          |
| 220.0          |
| 225.0          |
| 230.0          |
| 235.0          |
| 240.0          |
| 245.0          |
| 248.0          |
| 250.0          |
| 255.0          |
| 260.0          |
| 270.0          |
| 280.0          |
| 283.0          |
| 290.0          |
| 295.0          |
| 300.0          |
| 305.0          |
| 320.0          |
| 330.0          |
| 335.0          |
| 340.0          |
| 345.0          |
| 350.0          |
| 355.0          |
| 365.0          |
| 370.0          |
| 375.0          |
| 385.0          |
| 395.0          |
| 400.0          |
| 405.0          |
| 420.0          |
| 430.0          |
| 440.0          |
| 450.0          |
| 460.0          |
| 470.0          |
| 475.0          |
| 480.0          |
| 490.0          |
| 505.0          |
| 510.0          |
| 520.0          |
| 525.0          |
| 530.0          |
| 558.0          |
| 570.0          |
| 580.0          |
| 595.0          |
| 655.0          |
| 670.0          |
| 700.0          |
+----------------+
66 rows selected (30.636 seconds)


			*Arrondissement

0: jdbc:hive2://hadoop-master01.efrei.online:> select distinct arrondissement from trees_internal;
INFO  : Compiling command(queryId=hive_20211007222454_41353e6b-6f16-44f6-b7ee-ff16829bc51c): select distinct arrondissement from trees_internal
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+
| arrondissement  |
+-----------------+
| 3               |
| 4               |
| 5               |
| 6               |
| 7               |
| 8               |
| 9               |
| 11              |
| 12              |
| 13              |
| 14              |
| 15              |
| 16              |
| 17              |
| 18              |
| 19              |
| 20              |
+-----------------+
17 rows selected (31.65 seconds)


		*displays the list of different species trees:
0: jdbc:hive2://hadoop-master01.efrei.online:> select distinct espece from trees_internal;
INFO  : Compiling command(queryId=hive_20211007213929_196c7300-e4b0-4db2-9c36-8d4429490f85): select distinct espece from trees_internal
INFO  : Concurrency mode is disabled, not creating a lock manager
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+
|     espece      |
+-----------------+
| araucana        |
| atlantica       |
| australis       |
| baccata         |
| bignonioides    |
| biloba          |
| bungeana        |
| cappadocicum    |
| carpinifolia    |
| colurna         |
| coulteri        |
| decurrens       |
| dioicus         |
| distichum       |
| excelsior       |
| fraxinifolia    |
| giganteum       |
| giraldii        |
| glutinosa       |
| grandiflora     |
| hippocastanum   |
| ilex            |
| involucrata     |
| japonicum       |
| kaki            |
| libanii         |
| monspessulanum  |
| nigra           |
| nigra laricio   |
| opalus          |
| orientalis      |
| papyrifera      |
| petraea         |
| pomifera        |
| pseudoacacia    |
| sempervirens    |
| serrata         |
| stenoptera      |
| suber           |
| sylvatica       |
| tomentosa       |
| tulipifera      |
| ulmoides        |
| virginiana      |
| x acerifolia    |
+-----------------+
45 rows selected (31.116 seconds)


		*the number of trees of each kind:
0: jdbc:hive2://hadoop-master01.efrei.online:> Select variete, COUNT(*) from trees_internal group by variete;
INFO  : Compiling command(queryId=hive_20211007215829_6fff452c-5f6f-45fa-8368-cc9d285377b5): Select variete, COUNT(*) from trees_internal group by variete
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+------+
|     variete     | _c1  |
+-----------------+------+
|                 | 86   |
| Austriaca       | 1    |
| Glauca          | 1    |
| Glauca pendula  | 1    |
| Pendula         | 3    |
| Purpurea        | 4    |
| Tortuosa        | 1    |
+-----------------+------+
7 rows selected (30.05 seconds)



		*calculates the height of the tallest tree of each kind:

0: jdbc:hive2://hadoop-master01.efrei.online:> Select espece, MAX(hauteur) from trees_internal group by espece;
INFO  : Compiling command(queryId=hive_20211007215905_46aa35d0-5e40-46b6-9d3c-4e541fc83377): Select espece, MAX(hauteur) from trees_internal group by espece
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+-------+
|     espece      |  _c1  |
+-----------------+-------+ 
| araucana        | 9     |
| atlantica       | 25    |
| australis       | 16    |
| baccata         | 13    |
| bignonioides    | 15    |
| biloba          | 33    |
| bungeana        | 10    |
| cappadocicum    | 16    |
| carpinifolia    | 30    |
| colurna         | 20    |
| coulteri        | 14    |
| decurrens       | 20    |
| dioicus         | 10    |
| distichum       | 35    |
| excelsior       | 30    |
| fraxinifolia    | 27    |
| giganteum       | 35    |
| giraldii        | 35    |
| glutinosa       | 16    |
| grandiflora     | 12    |
| hippocastanum   | 30    |
| ilex            | 15    |
| involucrata     | 12    |
| japonicum       | 10    |
| kaki            | 14    |
| libanii         | 30    |
| monspessulanum  | 12    |
| nigra           | 30    |
| nigra laricio   | 30    |
| opalus          | 15    |
| orientalis      | 34    |
| papyrifera      | 12    |
| petraea         | 31    |
| pomifera        | 13    |
| pseudoacacia    | 11    |
| sempervirens    | 30    |
| serrata         | 18    |
| stenoptera      | 30    |
| suber           | 10    |
| sylvatica       | 30    |
| tomentosa       | 20    |
| tulipifera      | 35    |
| ulmoides        | 12    |
| virginiana      | 14    |
| x acerifolia    | 45    |
+-----------------+-------+
45 rows selected (31.268 seconds)


		*sort the trees height from smallest to largest:

0: jdbc:hive2://hadoop-master01.efrei.online:> Select objetcid, hauteur from trees_internal order by hauteur ASC;
INFO  : Compiling command(queryId=hive_20211007222752_866c8400-003e-4fb3-a526-376c1fb22859): Select objetcid, hauteur from trees_internal order by hauteur ASC
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:objetcid, type:int, comment:null), FieldSchema(name:hauteur, type:int, comment:null)], properties:null)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------+----------+
| objetcid  | hauteur  |
+-----------+----------+
| 67        | NULL     |
| 3         | 2        |
| 89        | 5        |
| 62        | 6        |
| 39        | 9        |
| 44        | 10       |
| 32        | 10       |
| 61        | 10       |
| 63        | 10       |
| 95        | 10       |
| 4         | 11       |
| 93        | 12       |
| 50        | 12       |
| 58        | 12       |
| 7         | 12       |
| 48        | 12       |
| 66        | 12       |
| 33        | 12       |
| 71        | 12       |
| 6         | 13       |
| 36        | 13       |
| 96        | 14       |
| 68        | 14       |
| 94        | 14       |
| 91        | 15       |
| 5         | 15       |
| 70        | 15       |
| 2         | 15       |
| 98        | 15       |
| 75        | 16       |
| 78        | 16       |
| 16        | 16       |
| 28        | 16       |
| 23        | 18       |
| 64        | 18       |
| 83        | 18       |
| 60        | 18       |
| 11        | 20       |
| 12        | 20       |
| 20        | 20       |
| 51        | 20       |
| 8         | 20       |
| 15        | 20       |
| 87        | 20       |
| 1         | 20       |
| 34        | 20       |
| 35        | 20       |
| 43        | 20       |
| 13        | 20       |
| 88        | 22       |
| 14        | 22       |
| 47        | 22       |
| 10        | 22       |
| 86        | 22       |
| 18        | 23       |
| 49        | 25       |
| 84        | 25       |
| 97        | 25       |
| 31        | 25       |
| 24        | 25       |
| 92        | 25       |
| 73        | 26       |
| 42        | 27       |
| 65        | 27       |
| 85        | 28       |
| 72        | 30       |
| 54        | 30       |
| 37        | 30       |
| 38        | 30       |
| 41        | 30       |
| 22        | 30       |
| 55        | 30       |
| 77        | 30       |
| 76        | 30       |
| 19        | 30       |
| 27        | 30       |
| 25        | 30       |
| 30        | 30       |
| 29        | 30       |
| 69        | 30       |
| 52        | 30       |
| 59        | 30       |
| 80        | 31       |
| 9         | 31       |
| 82        | 32       |
| 46        | 33       |
| 45        | 34       |
| 56        | 35       |
| 17        | 35       |
| 53        | 35       |
| 57        | 35       |
| 81        | 35       |
| 74        | 40       |
| 40        | 40       |
| 26        | 40       |
| 90        | 42       |
| 21        | 45       |
+-----------+----------+
97 rows selected (27.162 seconds)



		*displays the district where the oldest tree is:

0: jdbc:hive2://hadoop-master01.efrei.online:> Select arrondissement, annee_plant from trees_internal where annee_plant is not null order by annee_plant asc limit 1;
INFO  : Compiling command(queryId=hive_20211007222923_5e5605c3-165f-4d6c-968e-13b67fb50c62): Select arrondissement, annee_plant from trees_internal where annee_plant is not null order by annee_plant asc limit 1
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
...
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+--------------+
| arrondissement  | annee_plant  |
+-----------------+--------------+
| 5               | 1601         |
+-----------------+--------------+
1 row selected (32.512 seconds)


		*displays the district that contains the most trees:

0: jdbc:hive2://hadoop-master01.efrei.online:> Select arrondissement, COUNT(*) AS nb_trees from trees_internal group by arrondissement order by nb_trees desc limit 1;
INFO  : Compiling command(queryId=hive_20211007223005_463e6175-31b8-4b24-8e98-fb88849f6f46): Select arrondissement, COUNT(*) AS nb_trees from trees_internal group by arrondissement order by nb_trees desc limit 1
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:arrondissement, type:int, comment:null), FieldSchema(name:nb_trees, type:bigint, comment:null)], properties:null)
...
INFO  : Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 7.12 sec   HDFS Read: 31229 HDFS Write: 437 SUCCESS
INFO  : Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 7.37 sec   HDFS Read: 7983 HDFS Write: 105 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 14 seconds 490 msec
INFO  : Completed executing command(queryId=hive_20211007223005_463e6175-31b8-4b24-8e98-fb88849f6f46); Time taken: 57.236 seconds
INFO  : OK
INFO  : Concurrency mode is disabled, not creating a lock manager
+-----------------+-----------+
| arrondissement  | nb_trees  |
+-----------------+-----------+
| 16              | 36        |
+-----------------+-----------+
1 row selected (57.566 seconds)


