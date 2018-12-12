<a href="https://rbcdsai.iitm.ac.in/"><img title="RBC-DSAI logo" src="https://github.com/RBC-DSAI-IITM/rbc-dsai-iitm.github.io/blob/master/images/logo.jpg" height="200" width="351"></a>

# Parallelized game theoretic centrality algorithms

This repository contains parallelized implementations of the five game theoretic centrality algorithms proposed in [Tomasz Michalak et al (2013)](https://doi.org/10.1613/jair.3806).

## Getting started

1. Ensure that Java 8 is installed in your system. If not, please head over to [Java 8 Downloads](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and install Java SE Development Kit before proceeding. Run `java -version` to enure proper installation.
2. Clone this repository to your system and change your working directory to the cloned one.
3. Ensure that you have a working Hadoop cluster with everything configured. Please make sure that you are running [Hadoop 3.1.1](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz). Run `hadoop version` to ensure proper version.
4. Ensure that you have `maven` installed. If not, please check [Maven](https://maven.apache.org) for installation instructions. Run `mvn -v` to ensure proper installation.
5. Each of the directories named `Game{x}, where x = 1, 2, 3, 4, 5`; contain the source code of the algorithms. Head over to any of the directory in your cloned repo and run `mvn clean package`. This will generate a `Game{x}-1.0.0.jar`.
6. Run the jar using `hadoop jar Game{x}-1.0.0.jar in.ac.iitm.rbcdsai.centrality.Game{x}.Main <hdfs/path/to/input> <hdfs/path/to/output>`.

## Citation

If you use the implementations, please cite [SANKAR, M.V. & RAVINDRAN, B. Sadhana (2015) 40: 1821](https://doi.org/10.1007/s12046-015-0425-z).

Here's also a BibTeX entry for the publication:

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
