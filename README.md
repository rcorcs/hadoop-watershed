hadoop-watershed
================

Hadoop Watershed is a distributed system for efficient large-scale data stream processing on computer clusters.

== Proposal ==

Hadoop Watershed is a distributed stream processing system for large-scale data streams, inspired in the data-flow model.
Stream processing systems comprise a collection of modules that compute in parallel, and that communicate via data stream channels.
It allows for both continuous stream processing and batch processing applications.
The user is able to specify the entire application pipeline (or DAG), including the processing and communication components.

== Background ==

Watershed started as a research project at Universidade Federal de Minas Gerais (UFMG), Brazil.
The batch processing model incorporated by Hadoop Watershed is based on the Anthill framework, which is another research project developed at UFMG.

