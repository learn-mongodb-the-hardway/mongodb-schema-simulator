Message flow

Worker
 - initialize
 - send register message to master
 - receive configure message from master
 - send ready message to master
 
 Master
 - wait until we have x remote servers
 - send tick to each slave until done
 - send done to each slave and wait for responses
