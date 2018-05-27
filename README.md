Project 2 starter code
Copyright (C) George Porter, 2017, 2018.

## Overview

This is the starter code for the Java implementation of SurfStore.

## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To run unit test (JUnit 5)

- mvn test
The test source files are located under "/java/src/test/java"
- Please put all your test files under this folder
You may duplicate the test framework. To test server, need to start a Server
Thread and call RPC to this test server.

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
