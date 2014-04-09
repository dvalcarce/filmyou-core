filmyou-core
============

FilmYou is a movie recommender platform. This is part of my final degree project.

The aim of this project is to build a highly scalable distributed platform for movie recommendation using the following technologies:
 - Apache Hadoop
 - Apache Mahout
 - Apache Cassandra
 - Trove4j


Recommendation algorithms
-------------------------

Currently, filmyou-core contains two recommendation algorithms:

### Mahout's item-based collaborative filtering
This is the classical approach to CF. For each item, a set of similar items is computed. 

### Relevance-models-based collaborative filtering
This is a novel approach [1] that applies information retrieval techniques to the recommendation process achieving outstanding performance.




[1] Javier Parapar, Alejandro Bellogín, Pablo Castells and Álvaro Barreiro. Relevance-based language modelling for recommender systems. *Information Processing & Management*, 49(4):966–980, July 2013

