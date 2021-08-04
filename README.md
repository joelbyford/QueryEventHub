# QueryEventHub
A simple command line tool to query an Azure EventHub with a Connection String to read what has been posted there.

## Usage
Once built, can call by executing `QueryEventHub --connString "<EventHubConnectionString>" --name "<EventHubHubName>"`  to see all parameters simply enter `QueryEventHub --help`.  If not provided, the timeout will be 5 seconds.  
