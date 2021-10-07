LAB1_YARN MapReduce Big data framework
NOM & PRENOMS: Andrea Roxane ANEY
NUMERO ETUDIANT: 20200644
GROUPE TP : DS4


	1 YARN
In this tutorial, we will use MapReduce examples to learn how to work with Apache YARN.

1.1 The MapReduce examples
	*The samples are located on the Hortonworks Data Platform cluster at:
 /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar
	*Source code for these samples is included on the ODP cluster at:
 /usr/odp/current/hadoop-client/src/hadoop-mapreduce-project/hadoop-mapreduce-examples

1.2 Run the wordcount example
 	*Let's connect to HADOOP cluster using SSH.

Using username "a.aney".
Keyboard-interactive authentication prompts from server:
End of keyboard-interactive prompts from server
Access denied
Keyboard-interactive authentication prompts from server:
| Password:
End of keyboard-interactive prompts from server
Last failed login: Tue Oct  5 00:57:21 CEST 2021 from 185.230.76.135 on ssh:notty
There was 1 failed login attempt since the last successful login.
Last login: Wed Sep 29 15:17:43 2021 from nat-in-drelab.groupe-efrei.fr


 	*From the SSH session, let's use the following command to list the samples:

$ yarn jar / usr /odp/ current /hadoop-mapreduce-client/hadoop-mapreduce-examples.jar
This command generates the list of sample:

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar
An example program must be given as the first argument.
Valid program names are:
  aggregatewordcount: An Aggregate based map/reduce program that counts the words in the input files.
  aggregatewordhist: An Aggregate based map/reduce program that computes the histogram of the words in the input files.
  bbp: A map/reduce program that uses Bailey-Borwein-Plouffe to compute exact digits of Pi.
  dbcount: An example job that count the pageview counts from a database.
  distbbp: A map/reduce program that uses a BBP-type formula to compute exact bits of Pi.
  grep: A map/reduce program that counts the matches of a regex in the input.
  join: A job that effects a join over sorted, equally partitioned datasets
  multifilewc: A job that counts words from several files.
  pentomino: A map/reduce tile laying program to find solutions to pentomino problems.
  pi: A map/reduce program that estimates Pi using a quasi-Monte Carlo method.
  randomtextwriter: A map/reduce program that writes 10GB of random textual data per node.
  randomwriter: A map/reduce program that writes 10GB of random data per node.
  secondarysort: An example defining a secondary sort to the reduce.
  sort: A map/reduce program that sorts the data written by the random writer.
  sudoku: A sudoku solver.
  teragen: Generate data for the terasort
  terasort: Run the terasort
  teravalidate: Checking results of terasort
  wordcount: A map/reduce program that counts the words in the input files.
  wordmean: A map/reduce program that counts the average length of the words in the input files.
  wordmedian: A map/reduce program that counts the median length of the words in the input files.
  wordstandarddeviation: A map/reduce program that counts the standard deviation of the length of the words in the input files.


 	*Let's use the following command to get help on a specific sample. In this case,
the wordcount sample:
$ yarn jar / usr /odp/ current /hadoop-mapreduce-client /hadoop-mapreduce-examples.jar \
wordcount

We obtain the following message:
[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \wordcount
Usage: wordcount <in> [<in>...] <out>

NOTE: This message indicates that we can provide several input paths for the source documents.
The final path is where the output (count of words in the source documents) is stored.

	*Let's use the following to count all words in downloaded e-book (in Plain Text UTF-8) from Project Gutenberg:

$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \
wordcount /user/<username>/davinci.txt / user/<username>/wordcount

NOTE: The link has expired. I uploaded a Da vinci file to Project Gutenberg.
As you can see in the following, I renamed the file.
[a.aney@hadoop-edge01 ~]$ wget https://www.gutenberg.org/files/34300/34300-0.txt
--2021-10-05 16:37:16--  https://www.gutenberg.org/files/34300/34300-0.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:443... connected.
Unable to establish SSL connection.
[a.aney@hadoop-edge01 ~]$ ls
34300-0.txt  84  84-0.txt  index.html  local.txt
[a.aney@hadoop-edge01 ~]$ mv 34300-0.txt davinci.txt
[a.aney@hadoop-edge01 ~]$ ls
84  84-0.txt  davinci.txt  index.html  local.txt

NOTE: I then used the given command and got several lines (I just copied some first and last lines):

[a.aney@hadoop-edge01 ~]$ hdfs dfs -put davinci.txt
[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \wordcount /user/a.aney/davinci.txt /user/a.aney/wordcount
21/10/05 16:42:15 INFO impl.TimelineReaderClientImpl: Initialized TimelineReader URI=https://hadoop-master03.efrei.online:8199/ws/v2/timeline/, clusterId=yarn-cluster
21/10/05 16:42:15 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.102.23:10200
21/10/05 16:42:15 INFO hdfs.DFSClient: Created token for a.aney: HDFS_DELEGATION_TOKEN owner=a.aney@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1633444935818, maxDate=1634049735818, sequenceNumber=3407, masterKeyId=48 on ha-hdfs:efrei
21/10/05 16:42:15 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for a.aney: HDFS_DELEGATION_TOKEN owner=a.aney@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1633444935818, maxDate=1634049735818, sequenceNumber=3407, masterKeyId=48)
21/10/05 16:42:15 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2
21/10/05 16:42:15 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/a.aney/.staging/job_1630864376208_2308
21/10/05 16:42:16 INFO input.FileInputFormat: Total input files to process : 1
21/10/05 16:42:16 INFO mapreduce.JobSubmitter: number of splits:1
21/10/05 16:42:16 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1630864376208_2308
21/10/05 16:42:16 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for a.aney: HDFS_DELEGATION_TOKEN owner=a.aney@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1633444935818, maxDate=1634049735818, sequenceNumber=3407, masterKeyId=48)]
...
Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=166298
        File Output Format Counters
                Bytes Written=64827


NOTE: Input for this job is read from /user/<username>/davinci.txt. 
Output for this example is stored in /user/<username>/wordcount. 
Both paths are located on default storage for the cluster, not the local file system.

 	*Now let's use the following command to view the output:
$ hdfs dfs -cat /user/<username>/wordcount/part-r-00000

[a.aney@hadoop-edge01 ~]$ hdfs dfs -cat /user/a.aney/wordcount/part-r-00000

NOTE: This command concatenates all the output files produced by the job. It
displays the output to the console. The output is indeed similar to the given example. 
I got:
whose   13
why     5
wide    1
widely  1
widening        1
widest  3
wife    2
wife.   1
wild    1
will    28

NOTE: Each line represents a word and how many times it occurred in the input
data.

1.3 The Sudoku example

Sudoku is a logic puzzle made up of nine 3x3 grids. Some cells in the grid have
numbers, while others are blank, and the goal is to solve for the blank cells.
The previous link has more information on the puzzle, but the purpose of this
sample is to solve for the blank cells.

So our input should be a file that is in the following format:
 Nine rows of nine columns; Each column can contain either a number or ? (which indicates a blank
cell); Cells are separated by a space;
There is a certain way to construct Sudoku puzzles; you can’t repeat a number
in a column or row.

	* Let's create a file called sudoku.dta on our home local directory:

[a.aney@hadoop-edge01 ~]$ cat > sudoku.dta
8 5 ? 3 9 ? ? ? ?
? ? 2 ? ? ? ? ? ?
? ? 6 ? 1 ? ? ? 2
? ? 4 ? ? 3 ? 5 9
? ? 8 9 ? 1 4 ? ?
3 2 ? 4 ? ? 8 ? ?
9 ? ? ? 8 ? 5 ? ?
? ? ? ? ? ? 2 ? ?
? ? ? ? 4 5 ? 7 8

NOTE: We notice that the file has been created
[a.aney@hadoop-edge01 ~]$ ls
84  84-0.txt  davinci.txt  index.html  local.txt  sudoku.dta


	*Let's run this example problem:
$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \
sudoku sudoku.dta

NOTE: We obtain the following result

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \sudoku sudoku.dta
Solving sudoku.dta
8 5 1 3 9 2 6 4 7
4 3 2 6 7 8 1 9 5
7 9 6 5 1 4 3 8 2
6 1 4 8 2 3 7 5 9
5 7 8 9 6 1 4 2 3
3 2 9 4 5 7 8 1 6
9 4 7 2 8 6 5 3 1
1 8 5 7 3 9 2 6 4
2 6 3 1 4 5 9 7 8


1.4 Pi example

The pi sample uses a statistical (quasi-Monte Carlo) method to estimate the value of pi.
Points are placed at random in a unit square.
The square also contains a circle. The probability that the points fall within the circle is equal to the area of the circle, pi/4.
The value of pi can be estimated from the value of 4R. R is the ratio of the
number of points that are inside the circle to the total number of points that are within the square.
The larger the sample of points used, the better the estimate is.

	*  Let's use the following command to run this sample. 
NOTE: This command uses 16 maps with 10,000,000 samples each to estimate the value of pi:
$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \pi 16 10000000

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \pi 16 10000000

NOTE: We obtained:

Number of Maps  = 16
Samples per Map = 10000000
Wrote input for Map #0
Wrote input for Map #1
Wrote input for Map #2
Wrote input for Map #3
Wrote input for Map #4
Wrote input for Map #5
Wrote input for Map #6
Wrote input for Map #7
Wrote input for Map #8
Wrote input for Map #9
Wrote input for Map #10
Wrote input for Map #11
Wrote input for Map #12
Wrote input for Map #13
Wrote input for Map #14
Wrote input for Map #15
Starting Job
...
21/10/05 17:27:58 INFO mapreduce.Job:  map 0% reduce 0%
21/10/05 17:28:07 INFO mapreduce.Job:  map 38% reduce 0%
21/10/05 17:28:08 INFO mapreduce.Job:  map 100% reduce 0%
21/10/05 17:28:12 INFO mapreduce.Job:  map 100% reduce 100%
21/10/05 17:28:12 INFO mapreduce.Job: Job job_1630864376208_2328 completed successfully
...
Job Finished in 24.248 seconds
Estimated value of Pi is 3.14159155000000000000

NOTE:The value returned by this command is similar to 3.14159155000000000000. For references, the first 10 decimal places of pi are 3.1415926535.

1.5 10 GB GraySort example
GraySort is a benchmark sort. The metric is the sort rate (TB/minute) that is achieved while sorting large amounts of data, usually a 100 TB minimum.
This sample uses a modest 10 GB of data so that it can be run relatively quickly. 
It uses the MapReduce applications developed by Owen O’Malley and Arun Murthy.

	*Let's use the following steps to generate data, sort, and then validate the output:

Let's generate 10 GB of data, which is stored to our home directory at /user/<username>/data/10GB-sort-input:
$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \
teragen -Dmapred.map.tasks=50 100000000 /user/<username>/data/10GB-sort-input

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \teragen -Dmapred.map.tasks=50 100000000 /user/a.aney/data/10GB-sort-input
21/10/05 17:46:34 INFO impl.TimelineReaderClientImpl: Initialized TimelineReader URI=https://hadoop-master03.efrei.online:8199/ws/v2/timeline/, clusterId=yarn-cluster
21/10/05 17:46:34 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.102.23:10200
...
21/10/05 17:47:57 INFO mapreduce.Job:  map 98% reduce 0%
21/10/05 17:47:58 INFO mapreduce.Job:  map 99% reduce 0%
21/10/05 17:47:59 INFO mapreduce.Job:  map 100% reduce 0%
21/10/05 17:47:59 INFO mapreduce.Job: Job job_1630864376208_2349 completed successfully
...
  Job Counters
                Launched map tasks=50
                Other local map tasks=50
                Total time spent by all maps in occupied slots (ms)=8426208
                Total time spent by all reduces in occupied slots (ms)=0
                Total time spent by all map tasks (ms)=2808736
                Total vcore-milliseconds taken by all map tasks=2808736
                Total megabyte-milliseconds taken by all map tasks=4314218496
        Map-Reduce Framework
                Map input records=100000000
                Map output records=100000000
                Input split bytes=4288
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=8820
                CPU time spent (ms)=638220
                Physical memory (bytes) snapshot=14207111168
                Virtual memory (bytes) snapshot=170313711616
                Total committed heap usage (bytes)=14004256768
                Peak Map Physical memory (bytes)=302870528
                Peak Map Virtual memory (bytes)=3422953472
        org.apache.hadoop.examples.terasort.TeraGen$Counters
                CHECKSUM=214760662691937609
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=10000000000


NOTE: The -Dmapred.map.tasks tells Hadoop how many map tasks to use for
this job. The final two parameters instruct the job to create 10 GB of
data and to store it at /user/<username>/data/10GB-sort-input.
 

	*Let's use a command to sort the data:

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \terasort -Dmapred.map.tasks=50 -Dmapred.reduce.tasks=25 \/user/a.aney/data/10GB-sort-input /user/a.aney/data/10GB-sort-output
21/10/05 18:05:45 INFO terasort.TeraSort: starting
21/10/05 18:05:46 INFO hdfs.DFSClient: Created token for a.aney: HDFS_DELEGATION_TOKEN owner=a.aney@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1633449946091, maxDate=1634054746091, sequenceNumber=3522, masterKeyId=48 on ha-hdfs:efrei
...
21/10/05 18:07:43 INFO mapreduce.Job:  map 100% reduce 97%
21/10/05 18:07:44 INFO mapreduce.Job:  map 100% reduce 100%
21/10/05 18:07:44 INFO mapreduce.Job: Job job_1630864376208_2378 completed successfully
...
 Map-Reduce Framework
                Map input records=100000000
                Map output records=100000000
                Map output bytes=10200000000
                Map output materialized bytes=10400015000
                Input split bytes=12300
                Combine input records=0
                Combine output records=0
                Reduce input groups=100000000
                Reduce shuffle bytes=10400015000
                Reduce input records=100000000
                Reduce output records=100000000
                Spilled Records=200000000
                Shuffled Maps =2500
                Failed Shuffles=0
                Merged Map outputs=2500
                GC time elapsed (ms)=132868
                CPU time spent (ms)=1979470
                Physical memory (bytes) snapshot=135959101440
                Virtual memory (bytes) snapshot=437367545856
                Total committed heap usage (bytes)=140477726720
                Peak Map Physical memory (bytes)=1201000448
                Peak Map Virtual memory (bytes)=3405393920
                Peak Reduce Physical memory (bytes)=1066934272
                Peak Reduce Virtual memory (bytes)=3901120512
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=10000000000
        File Output Format Counters
                Bytes Written=10000000000
21/10/05 18:07:44 INFO terasort.TeraSort: done

NOTE: The -Dmapred.reduce.tasks tells Hadoop how many reduce tasks to use for the job.
The final two parameters are just the input and output locations for data.


	*Let's use a command to validate the data generated by the sort:

[a.aney@hadoop-edge01 ~]$ yarn jar /usr/odp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar \teravalidate -Dmapred.map.tasks=50 -Dmapred.reduce.tasks=25 \/user/a.aney/data/10GB-sort-output /user/a.aney/data/10GB-sort-validate
21/10/05 18:13:27 INFO impl.TimelineReaderClientImpl: Initialized TimelineReader URI=https://hadoop-master03.efrei.online:8199/ws/v2/timeline/, clusterId=yarn-cluster
21/10/05 18:13:28 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.102.23:10200
...
21/10/05 18:13:53 INFO mapreduce.Job:  map 100% reduce 100%
21/10/05 18:13:53 INFO mapreduce.Job: Job job_1630864376208_2395 completed successfully
21/10/05 18:13:54 INFO mapreduce.Job: Counters: 54
...
   File Input Format Counters
                Bytes Read=10000000000
        File Output Format Counters
                Bytes Written=25


2 MapReduce2
In this tutorial I will describe how to write a simple MapReduce program for
Hadoop in the Python programming language.

2.1  Motivation
Even though the Hadoop framework is written in Java, programs for Hadoop do not have to be coded in Java.
They can also be developed in other languages such as Python or C++ (the latter since version 0.14.1).
The goal of this tutorial is to write a Hadoop MapReduce program.

2.2  What we want to do
We will write a simple MapReduce program for Hadoop in Python, but without using Jython to translate our
code into Java jar files.
Our program will mimic WordCount, that is, it will read text files and count how often words appear.
Note: we can also use programming languages other than Python, such as Perl or Ruby.

2.3  Python MapReduce Code

The “trick” behind the following Python code is that we will use the Hadoop Streaming API 
for helping us passing data between our Map and Reduce code via STDIN (standard input) and STDOUT (standard output)
We will simply use Python’s sys.stdin to read input data and print our own output to sys.stdout.

2.3.1 Map step: mapper.py
	*Let's save the following code in the file /home/<username>/mapper.py:


NOTE:It will read data from STDIN, split it into words and output a list of lines
mapping words to their (intermediate) counts to STDOUT.



	*Let's save a code in the file /home/<username>/mapper.py:

[a.aney@hadoop-edge01 ~]$ cat > mapper.py
#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        words=line.split()
        # increase counters
        for word in words:
                #write the results to STDOUT (standard output);
                # what we output here will be the input for the
                #Reduce step, i.e. the input for reducer.py
                #
                # tab-delimited;the trivial word count is 1
                print '%s\t%s' % (word,1)

NOTE:It will read data from STDIN, split it into words and output a list of lines
mapping words to their (intermediate) counts to STDOUT.

[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt  local.txt  sudoku.dta
84-0.txt  index.html   mapper.py

NOTE: Je vais mettre le fichier qui se trouve dans home directory dans mon environnement hdfs
[a.aney@hadoop-edge01 ~]$ kinit
Password for a.aney@EFREI.ONLINE:
[a.aney@hadoop-edge01 ~]$ hdfs dfs -put mapper.py
-rw-r--r--   3 a.aney a.aney        502 2021-10-06 14:06 mapper.py

	*Let's make sure the file has execution permission:

[a.aney@hadoop-edge01 ~]$ chmod +x /home/a.aney/mapper.py          
[a.aney@hadoop-edge01 ~]$


2.3.2 Reduce step: reducer.py

	*Let's save a code in the file /home/<username>/mapper.py:

[a.aney@hadoop-edge01 ~]$ cat > reducer.py
#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word= None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        #parse the input we got from mapper.py
        word, count = line.split('\t',1)
        #convert count (currently a string) to int
        try:
                count = int(count)
        except ValueError:
                continue
        # this IF-switch only works because Hadoop sorts map output
        #by key (here: word) before it is passed to the reducer
        if current_word == word:
                current_count += count
        else:
                if current_word:
                        print '%s\t%s' % (current_word, current_count)
                current_count =count
                current_word = word
#do not forget to output the last word if needed!
if current_word == word:
        print '%s\t%s' % (current_word, current_count)

[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt  local.txt  reducer.py
84-0.txt  index.html   mapper.py  sudoku.dta

NOTE: I will put the reducer.py file that is in home directory in my hdfs environment
[a.aney@hadoop-edge01 ~]$ hdfs dfs -put reducer.py                
[a.aney@hadoop-edge01 ~]$ hdfs dfs -ls   
-rw-r--r--   3 a.aney a.aney        835 2021-10-06 14:43 reducer.py

	*Let's make sure the file has execution permission:
[a.aney@hadoop-edge01 ~]$ chmod +x /home/a.aney/reducer.py
[a.aney@hadoop-edge01 ~]$


2.3.3 Let's test our code (cat data | map | sort | reduce)
[a.aney@hadoop-edge01 ~]$ echo "foo foo quuux labs foo bar quux" | /home/a.aney/mapper.py
NOTE: I obtained:
foo     1
foo     1
quuux   1
labs    1
foo     1
bar     1
quux    1

NOTE: I tried with other texts and I get:
[a.aney@hadoop-edge01 ~]$ echo "efrei paris ecole d'ingenieur du numerique efrei propose un enseignement de qualité avec des enseignants qualifiés merci Efrei paris avec efrei j'apprend beaucoup. Je peux telecharger un e-book de project gutenberg au lieu d'ecrire des textes moi-meme" | /home/a.aney/mapper.py
efrei   1
paris   1
ecole   1
d'ingenieur     1
du      1
numerique       1

[a.aney@hadoop-edge01 ~]$ cat davinci.txt | /home/a.aney/mapper.py
Project 1
Gutenberg       1
License 1
included        1
with    1
this    1
eBook   1
or      1
online  1
at      1
...


[a.aney@hadoop-edge01 ~]$ echo "foo foo quux labs foo bar quux" | /home/a.aney/mapper.py | sort -k1,1 | /home/a.aney/reducer.py       
bar     1
foo     3
labs    1
quux    2

NOTE: as can be seen the code works as intended. I tried with another text:
[a.aney@hadoop-edge01 ~]$ echo "efrei efrei paris efr ecole du numerique" | /home/a.aney/mapper.py | sort -k1,1 | /home/a.aney/reducer.py
du      1
ecole   1
efr     1
efrei   2
numerique       1
paris   1

2.4 Running the Python Code on Hadoop

2.4.1 Download example input data

	*let's download three Project Gutenberg ebooks for this example and store the files in our home directory:
		* The Outline of Science, Vol. 1 (of 4) by J. Arthur Thomson

[a.aney@hadoop-edge01 ~]$ wget https://www.gutenberg.org/cache/epub/20417/pg20417.txt
--2021-10-06 19:32:26--  https://www.gutenberg.org/cache/epub/20417            /pg20417.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2            610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:            443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 674570 (659K) [text/plain]
Saving to: ‘pg20417.txt’

100%[=========================>] 674,570     1.38MB/s   in 0.5s

2021-10-06 19:32:33 (1.38 MB/s) - ‘pg20417.txt’ saved [674570/674570]

[a.aney@hadoop-edge01 ~]$ mv pg20417.txt j_a_thomson.txt
[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt  j_a_thomson.txt  mapper.py   sudoku.dta
84-0.txt  index.html   local.txt        reducer.py

NOTE: File uploaded and renamed to j_a_thomson.txt

		* The Notebooks of Leonardo Da Vinci

[a.aney@hadoop-edge01 ~]$ wget https://www.gutenberg.org/files/5000/5000-8.txt
--2021-10-06 19:53:29--  https://www.gutenberg.org/files/5000/5000-8.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 1428843 (1.4M) [text/plain]
Saving to: ‘5000-8.txt’

100%[=====================================>] 1,428,843   1.32MB/s   in 1.0s

2021-10-06 19:53:38 (1.32 MB/s) - ‘5000-8.txt’ saved [1428843/1428843]

[a.aney@hadoop-edge01 ~]$ mv 5000-8.txt d_v_leonardo.txt
[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt       index.html       local.txt  reducer.py
84-0.txt  d_v_leonardo.txt  j_a_thomson.txt  mapper.py  sudoku.dta

NOTE: File uploaded and renamed to d_v_leonardo.txt

		*Ulysses by James Joyce

[a.aney@hadoop-edge01 ~]$ wget https://www.gutenberg.org/files/4300/4300-0.txt
--2021-10-06 19:55:54--  https://www.gutenberg.org/files/4300/4300-0.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 1586336 (1.5M) [text/plain]
Saving to: ‘4300-0.txt’

100%[=====================================>] 1,586,336   1.50MB/s   in 1.0s

2021-10-06 19:56:02 (1.50 MB/s) - ‘4300-0.txt’ saved [1586336/1586336]

[a.aney@hadoop-edge01 ~]$ mv 4300-0.txt James_Joyce.txt
[a.aney@hadoop-edge01 ~]$ ls
84           d_v_leonardo.txt  j_a_thomson.txt  reducer.py
84-0.txt     index.html        local.txt        sudoku.dta
davinci.txt  James_Joyce.txt   mapper.py

NOTE:File uploaded and renamed to James_Joyce.txt

2.4.2 Copy local example data to HDFS

	*Let's copy the files from our local file system to Hadoop’s HDFS:
		* The Outline of Science, Vol. 1 (of 4) by J. Arthur Thomson
[a.aney@hadoop-edge01 ~]$ hdfs dfs -put j_a_thomson.txt
-rw-r--r--   3 a.aney a.aney     674570 2021-10-06 20:00 j_a_thomson.txt

		* The Notebooks of Leonardo Da Vinci
[a.aney@hadoop-edge01 ~]$ hdfs dfs -put d_v_leonardo.txt
-rw-r--r--   3 a.aney a.aney    1428843 2021-10-06 20:00 d_v_leonardo.txt

		*Ulysses by James Joyce
[a.aney@hadoop-edge01 ~]$ hdfs dfs -put James_Joyce.txt
-rw-r--r--   3 a.aney a.aney    1586336 2021-10-06 20:00 James_Joyce.txt


2.4.3 Run the MapReduce job

NOTE:After uploading the files to my home directory and putting them on the Hadoop cluster,
I will copy the 3 files in document that I will call gutenberg and I will transfer the document 
sur le cluster Hadoop.
[a.aney@hadoop-edge01 ~]$ mv j_a_thomson.txt gutenberg
[a.aney@hadoop-edge01 ~]$ mv d_v_leonardo.txt gutenberg
[a.aney@hadoop-edge01 ~]$ mv James_Joyce.txt gutenberg
[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt  index.html  mapper.py   sudoku.dta
84-0.txt  gutenberg    local.txt   reducer.py

[a.aney@hadoop-edge01 ~]$ hdfs dfs -copyFromLocal gutenberg /user/a.aney/gutenberg
[a.aney@hadoop-edge01 ~]$ hdfs dfs -ls
-rw-r--r--   3 a.aney a.aney    1586336 2021-10-06 22:02 gutenberg

[a.aney@hadoop-edge01 ~]$ ls
84        davinci.txt       index.html  reducer.py
84-0.txt  gutenberg         local.txt   sudoku.dta
dataset   gutenberg-output  mapper.py
[a.aney@hadoop-edge01 ~]$ hdfs dfs -copyFromLocal gutenberg-output /user/a.aney/gutenberg-output
[a.aney@hadoop-edge01 ~]$ hdfs dfs -ls
-rw-r--r--   3 a.aney a.aney          0 2021-10-07 21:18 gutenberg-output


2.4.4 Improved Mapper and Reducer code: using Python iterators and generators

The Mapper and Reducer examples above gave me an idea of how to create my first MapReduce application.

2.4.5 mapper.py

	*Let's write the code:

[a.aney@hadoop-edge01 ~]$ nano mapper.py

#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

import sys

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        for word in words:
            print '%s%s%d' % (word, separator, 1)

if __name__ == "__main__":
    main()


2.4.6 reducer.py

	*Let's write the code:

[a.aney@hadoop-edge01 ~]$ nano reducer.py
#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their gro$
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["&lt;current_word&gt;", "&lt;count$
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print "%s%s%d" % (current_word, separator, total_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()


