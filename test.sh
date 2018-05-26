#! /bin/bash
CONFIG=./configs
CENTRALIZED=$CONFIG/configCentralized.txt
DISTRIBUTED=$CONFIG/configDistributed.txt

# running client
./java/target/surfstore/bin/runMetadataStore $CENTRALIZED
