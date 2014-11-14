Hadoop Watershed
================

Hadoop Watershed is a distributed system for efficient large-scale data stream processing on computer clusters.

## Proposal

Hadoop Watershed is a distributed stream processing system for large-scale data streams, inspired in the data-flow model.
Stream processing systems comprise a collection of modules that compute in parallel, and that communicate via data stream channels.
It allows for both continuous stream processing and batch processing applications.
The user is able to specify the entire application pipeline (or DAG), including the processing and communication components.

## Background

Watershed started as a research project at Universidade Federal de Minas Gerais (UFMG), Brazil.
The batch processing model incorporated by Hadoop Watershed is based on the Anthill framework, which is another research project developed at UFMG.

## Dependencies

* [Hadoop YARN](http://hadoop.apache.org/)
* [Hadoop Distributed File System (HDFS)](http://hadoop.apache.org/) 
* [Apache ZooKeeper](http://zookeeper.apache.org/)
* [Netty](http://netty.io/)
* [Gson](https://code.google.com/p/google-gson/)
* [Apache Commons Codec](http://commons.apache.org/proper/commons-codec/)
* [Apache Commons CLI](http://commons.apache.org/proper/commons-cli/)

## References
### Framework
[Watershed reengineering: making streams programmable](http://homepages.dcc.ufmg.br/~rcor/wpba14.pdf)
[Watershed: A High Performance Distributed Stream Processing System](https://14b121fd-a-62cb3a1a-s-sites.googlegroups.com/site/rsilvaoliveira/home/2011-SBAC_PAD-watershed.pdf?attachauth=ANoY7crlFZFt2eyV6ER6ENyFNhlnaQ1mtA_wP660LrjkOF2CKBpzwtrL3o6VL8k510nb3f3MuzuEClRe0IlPVGK4wh0CgsAbXvf4rz7rGssUUK4fnNwcWwD7LtzsLvpGiH03WLKce1EjUoJdnWgYG2nZW31G9DQ8fCkzqaEQjQPsobfONPrrQ0ihT4c-pCNGMpuloHVhm5UCPRf5K-qcHq6sYKSE8n6GtDF4ICHxQDmogEcJ1WPIbjo%3D&attredirects=0)
[Anthill: A Scalable Run-Time Environment for Data Mining Applications](http://homepages.dcc.ufmg.br/~dorgival/artigos/sbac2005.pdf)
[AnthillSched: A Scheduling Strategy for Irregular and Iterative I/O-Intensive Parallel Jobs](http://homepages.dcc.ufmg.br/~pcalais/papers/JSSPP.pdf)
### Applications
[Twig: An Adaptable and Scalable Distributed FPGrowth](http://homepages.dcc.ufmg.br/~rcor/ipdps15.pdf)
[Distributed Skycube Computation with Anthill](http://homepages.dcc.ufmg.br/~lcerf/publications/articles/Distributed%20Skycube%20Computation%20with%20Anthill.pdf)

