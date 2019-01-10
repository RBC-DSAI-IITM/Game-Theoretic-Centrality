<a href="https://rbcdsai.iitm.ac.in/"><img title="RBC-DSAI logo" src="https://github.com/RBC-DSAI-IITM/rbc-dsai-iitm.github.io/blob/master/images/logo.jpg" height="200" width="351"></a>

# Parallelized game theoretic centrality algorithms

This repository contains parallelized implementations of the five game theoretic centrality algorithms proposed in [Tomasz Michalak et al (2013)](https://doi.org/10.1613/jair.3806). Please check the [publication](https://doi.org/10.1007/s12046-015-0425-z) for more details.

## Authors

1. M Vishnu Sankar
2. [Balaraman Ravindran](http://www.cse.iitm.ac.in/~ravi/)

## Getting started

### Building from source

1. Ensure that Java 8 is installed in your system. Run `java -version` to ensure proper installation. If not, please install [Java SE Development Kit 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) before proceeding. You can also use [OpenJDK](https://openjdk.java.net/install/) if you prefer that.
2. Ensure that you have `maven` installed. Run `mvn -v` to ensure proper installation. If not, please install [Maven](https://maven.apache.org/) following the official documentation.
3. Clone this repository to your system and change your working directory to the cloned one.
4. Each of the directories named `Game{x}, where x = 1, 2, 3, 4, 5` inside `src/`; contain the source code of the algorithms. Head over to the directories in your cloned repo and run `mvn clean package`. This will generate the `Game{x}-1.0.0.jar`.

### Using compiled executable files

1. Ensure that Java 8 is installed in your system. Run `java -version` to enure proper installation. If not, please install [Java SE Runtime Environment 8](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html) before proceeding. You can also use [OpenJDK](https://openjdk.java.net/install/) if you prefer that.
2. Please download the `.jar` files from [dist/](https://github.com/RBC-DSAI-IITM/Game-theoretic-centrality/tree/master/dist) and follow the Apache Hadoop setup instructions.

### Setting up Apache Hadoop

1. Please install [Apache Hadoop 3.1.1](https://hadoop.apache.org/release/3.1.1.html) and configure it according to the official documentation.
2. Ensure `hadoop version` is displaying the correct version.
3. Also, make sure you have a working HDFS cluster before proceeding.

## Usage

The `.jar` files need to be run like so:

```
$ hadoop jar Game{x}-1.0.0.jar in.ac.iitm.rbcdsai.centrality.Game{x}.Main <hdfs/path/to/input> <hdfs/path/to/output>
```

where the algorithm to run can be chosen by setting **x** to be one of  {1, 2, 3, 4, 5}.

### Input file format

The input file is basically an adjacency list containing edges of the graph. Each line denotes an edge, whose nodes are separated by a single whitespace as the delimiter.

A few things to also note about the format:
1. No comments or blank lines.
2. No annotations. 
3. No explicit mention of total nodes or edges.

For clarity, here's a basic example of what `input.txt` can be:

```
1 2
2 3
3 4
4 5
```

### Output format

The output directory name will depend on the number of steps required for the respective algorithm to process the input file. For example, `Game 1` has two sets of Map-Reduce, and so, the final output file will be in the directory path: `<hdfs/path/to/output>2`. The output file will have nodes listed along with the respective Shapley value, like so:

```
1 2.556
2 5.444
```

## Example

Here's an example of running `Game 1` on [email-EuAll](https://snap.stanford.edu/data/email-EuAll.html) dataset from [SNAP](https://snap.stanford.edu/), and input file being stored in HDFS, having input path as `/usr/hadoop/email-Eu-core.txt` and output directory path as `/usr/hadoop/email-Eu-core-output`:

```
$ hadoop jar Game1-1.0.0.jar in.ac.iitm.rbcdsai.centrality.Game1.Main '/usr/hadoop/email-Eu-core.txt' '/usr/hadoop/email-Eu-core-output'
```

## Citation

If you use the implementations, please cite:

```
@Article{SANKAR2015,
    author  = "SANKAR, M. VISHNU and RAVINDRAN, BALARAMAN",
    title   = "Parallelization of game theoretic centrality algorithms",
    journal = "Sadhana",
    year    = "2015",
    month   = "Sep",
    day     = "01",
    volume  = "40",
    number  = "6",
    pages   = "1821--1843",
    issn    = "0973-7677",
    doi     = "10.1007/s12046-015-0425-z",
    url     = "https://doi.org/10.1007/s12046-015-0425-z",
}
```
