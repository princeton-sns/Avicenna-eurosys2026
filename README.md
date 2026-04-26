Avicenna
======


### What is Avicenna?
Avicenna is a consensus protocol that tolerates 1 fail-slow replica without sacrificing normal-case latency.

Geo-distributed replicated state machines (RSMs) are at the heart of many production distributed systems, offering linearizability and fault tolerance via consensus protocols. Most existing protocols target crash fault tolerance, however, and are vulnerable to fail-slow faults, where a single slow replica can significantly degrade system latency. Existing protocols that tolerate fail-slow faults do so with much higher normalcase latency in geo-distributed settings.

Avicenna is the first consensus protocol for geo-distributed RSMs that maintains low normal-case latency while tolerating a single fail-slow replica. Avicenna uses a single leader to order commands, naturally tolerating a fail-slow follower. To tolerate a fail-slow leader, Avicenna compares the current latency with the counterfactual latency clients would experience if a different replica, the shadow leader, were the leader. When that comparison indicates the current leader might be slow, Avicenna quickly promotes the shadow leader with a fast leader rotation protocol. Our evaluation shows Avicenna has the same normal-case latency as Multi-Paxos while tolerating fail-slow faults.


### Avicenna Paper Reference

Christopher Hodsdon, Zijian Qin, Khiem Ngo, Siddhartha Sen, Ethan Katz-Bassett, Wyatt Lloyd. 2026. Avicenna: Masking Slowdowns in Replicated State Machines with Counterfactual Evaluation. In 21st European Conference on Computer Systems (EUROSYS ’26), April 27–30, 2026, Edinburgh, Scotland Uk. ACM, New York, NY, USA, 23 pages. https://doi.org/10.1145/3767295.3803615


### How to use this repository?

Please refer to the wiki page! 


### What is in this repository?

This repository contains the Go implementations of:

* Copilot

* Avicenna

* Latent Copilot

* EPaxos

* (classic) Paxos

* Mencius

* Generalized Paxos

The implementations of EPaxos, MultiPaxos, Mencius, and Generalized Paxos were created by Iulian Moraru, David G. Andersen, and Michael Kaminsky as part of the [EPaxos project](https://github.com/efficient/epaxos).

The struct marshaling and unmarshaling code was generated automatically using
the tool available at: https://code.google.com/p/gobin-codegen/


AUTHORS:

Christopher Hodsdon -- Databricks

Zijian Qin -- Princeton University

Khiem Ngo -- Datadog

Siddhartha Sen -- Microsoft Research

Ethan Katz-Bassett -- Columbia University

Wyatt Lloyd -- Princeton University


