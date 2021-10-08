LAB2_YARN MapReduce Big data framework
NOM & PRENOMS: Andrea Roxane ANEY
NUMERO ETUDIANT: 20200644
GROUPE TP : DS4

1. MapReduce JAVA

In this tutorial, we will use write MapReduce jobs using JAVA. Before that, we
will install prerequirements, if you already have them, skip thoses parts.

1.1 Install OpenJDK 8
1.1.1 Microsoft Windows

Let's download and install OpenJDK for Windows. 
After this, let's Start the installer and configure it to install everything.

1.1.2 Mac OS X (Note: I only deal with the Microsoft Windows part because I don't have a Mac)

1.2 Git

1.2.1 Microsoft Windows

Let's download and install 64-bit Git for Windows Setup

1.2.2 Mac OS X (Note: I only deal with the Microsoft Windows part because I don't have a Mac)

1.3 IntelliJ IDEA
1.3.1 Microsoft Windows

Note: I already had the IntelliJ application in my computer 
so it was not necessary to reinstall it

1.3.2 Mac OS X

(Note: I only deal with the Microsoft Windows part because I don't have a Mac)

1.4 Clone the project
Here is a JAVA project hosted on github to start your lab.

1.4.1 Microsoft Windows
Let's use Git GUI for cloning the repository.

1.4.2 Mac OS X
(Note: I only deal with the Microsoft Windows part because I don't have a Mac)

1.5 Import the project

I opened a new project(import) from the source cloned and i generate the JAR using maven lifecycle package.
I saw the building process and after an [INFO] BUILD SUCCESS message.
I saw a new folder package, inside this folder i found the JAR and JAR *-with-dependencies.jar(to be used).

1.6 Send the JAR to the edge node

1.6.1 Microsoft Windows

Let's download and install FileZilla client for Windows.
Let's connect to the edge node using thoses parameters:
*Host: sftp://hadoop-edge01.efrei.online
*Username: a.aney
*Password: ........
*Port: 22

1.6.2 Mac OS X
(Note: I only deal with the Microsoft Windows part because I don't have a Mac)


1.7 How will the lab work?

	*1.7.1 Important 1
For this lab, I started with the questions that seem easy to me.

	*1.7.2 Important 2
Unit test to be used.

	*1.7.3 Important 3
If you don’t have a GitHub account, you must create one. You must clone the
base repository following this tutorial (keep your repository public).
At the end of each lab session, you must add on Moodle a file with your commit
ID:
$ <name1>=<name2>=<commit=id >.m
Link : https://github.com/Andy2405/lab2

	*1.8 Remarkable trees of Paris
Let's write some MapReduce jobs on the remarkable trees of Paris
using this dataset=>https://github.com/makayel/hadoop-examples-mapreduce/blob/main/src/test/resources/data/trees.csv
Let's download the file and put it in our HDFS home directory.

*Write a MapReduce job that displays the list of distinct containing trees in this
file:

[a.aney@hadoop-edge01 ~]$ nano trees_mapreduce.java

public class trees_Mapper extends Mapper<Object, Text, Text, IntWritable> {
	public int curr_line = 0;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			context.write(new Text(value.toString().split(";")[1]), new IntWritable(1));
		}
		curr_line++;
	}
}

public class  trees_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
    	int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
    	context.write(key, new IntWritable(sum));
    }
}


- 1.8.2 Show all existing species 

[a.aney@hadoop-edge01 ~]$ nano species_mapreduce.java

public class species_Mapper extends Mapper<Object, Text, Text, NullWritable> {
	public int curr_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			context.write(new Text(value.toString().split(";")[3]), NullWritable.get());
		}
		curr_line++;
	}
}

public class species_Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}


-  1.8.3 Number of trees by kinds 

[a.aney@hadoop-edge01 ~]$ nano Number_mapreduce.java

public class Number_Mapper extends Mapper<Object, Text, Text, IntWritable> {
	public int curr_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			context.write(new Text(value.toString().split(";")[3]), new IntWritable(1));
		}
		curr_line++;
	}
}

public class Number_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}


- 1.8.4 Maximum height per kind of tree  

[a.aney@hadoop-edge01 ~]$ nano Maximum_height_mapreduce.java

public class Maximum_height_Mapper extends Mapper<Object, Text, Text, FloatWritable> {
	public int curr_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			try {
				Float height = Float.parseFloat(value.toString().split(";")[6]);
				context.write(new Text(value.toString().split(";")[3]), new FloatWritable(height));
			} catch (NumberFormatException ex) {
				// If the value is not a float, skip it by catching the error from the parseFloat() method
			}
		} curr_line++;
	}
}

public class Maximum_height_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public class HeightSpeciesReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    			throws IOException, InterruptedException {
    		context.write(key, new FloatWritable(StreamSupport.stream(values.spliterator(), false)
    				.map((v) -> { return v.get(); }).
    				max(Float::compare).get()));
    	}
    }

}


- 1.8.5 Sort the trees height from smallest to largest (average)
[a.aney@hadoop-edge01 ~]$ nano sort_mapreduce.java

public sort_Mapper extends Mapper<Object, Text, FloatWritable, Text> {
	public int curr_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			try {
				String[] line_tokens = value.toString().split(";");
				Float height = Float.parseFloat(line_tokens[6]);
				context.write(new FloatWritable(height),
						new Text(line_tokens[11] + " - " + line_tokens[2] + " " + line_tokens[3] + " (" + line_tokens[4] + ")"));
			} catch (NumberFormatException ex) {
				// If the value is not a float, skip by catching the error from the parseFloat() method
			}
		} curr_line++;
	}
}

public class sort_Reducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
	public void reduce(FloatWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			context.write(val, key);
		}
	}
}


-  1.8.6 District containing the oldest tree (difficult)
[a.aney@hadoop-edge01 ~]$ nano oldest_mapreduce.java

public class oldest_Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	public int curr_line = 0;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (curr_line != 0) {
			try {
				Integer year = Integer.parseInt(value.toString().split(";")[5]);
				context.write(new IntWritable(year), new IntWritable(Integer.parseInt(value.toString().split(";")[1])));
			} catch (NumberFormatException ex) {
				// If the year is not an integer, skip by catching the error from the parseFloat() method
			}
		} curr_line++;
	}
}

public class oldest_reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	public boolean first = true;

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		if (first) {
			StreamSupport.stream(values.spliterator(), false).distinct().forEach(v -> {
				try {
					context.write(key, v);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			});
		}
		first = false;
	}
}


- 1.8.7 District containing the most trees (very difficult)
[a.aney@hadoop-edge01 ~]$ nano Most_mapreduce.java

public class Most_Mapper extends Mapper<LongWritable, IntWritable, NullWritable, MapWritable> {
	public void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
		MapWritable map = new MapWritable();
		map.put(new IntWritable((int) key.get()), value);
		context.write(NullWritable.get(), map);
	}
}

public class Most_Reducer extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
		ArrayList<Integer[]> district_trees = (ArrayList<Integer[]>) StreamSupport.stream(values.spliterator(), false)
				.map( mw ->  (new Integer[] { ((IntWritable) mw.keySet().toArray()[0]).get(), ((IntWritable) mw.get(mw.keySet().toArray()[0])).get() }))
				.collect(Collectors.toList());
		// Copies the iterable to an arraylist so multiple operations can be done on the iterable

		int max_trees = district_trees.stream().map((arr) -> arr[1]).max(Integer::compare).get();

		district_trees.stream().filter(arr -> arr[1] == max_trees).map(arr -> arr[0]).distinct().forEach((district) -> { try {
			context.write(new IntWritable(max_trees), new IntWritable(district));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		} });
    }
}
