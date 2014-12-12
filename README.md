NgramTagStackoverflow
=====================

N-gram analysis over tags in stackoverflow dataset.

To clean:

	$ mvn clean
	
To compile:

	$ mvn compile
	
To create jar:

	$ mvn package
	
To create eclipse project:

	$ mvn eclipse:eclipse

To run using maven (only for standalone):

	$ mvn exec:java -Dexec.mainClass="ch.hesso.master.Ngram" -Dexec.args="NUM_REDUCERS INPUT_PATH OUTPUT_PATH"
	 
To run using eclipse (only for standalone):

	Run Configuration -> New Java Application -> Define the main class and arguments

To run using Hadoop (for both standalone and pseudo-distributed):

	$ ${HAOOP_HOME}/bin/hadoop jar JAR_FILE_PATH ch.hesso.master.Ngram NUM_REDUCERS INPUT_PATH OUTPUT_PATH

 