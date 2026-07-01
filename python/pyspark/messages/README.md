## PySpark <-> Spark message interface

This module implements message-based communication between PySpark and the Spark. 
It introduces abstraction layers, which handle the receiving and sending of messages.
Through these abstractions, the underlying data transport channel (Unix domain socket, gRPC, etc.)
 can be *decoupled from the core PySpark logic*, which processes the data.

Overall, introducing these abstractions allows the same PySpark code to work with
different underlying data transport channels transparently. 

This module defines the following message types:

### Spark -> PySpark

1. Initialization - UDF Payload, parameters, ...
2. Data - Data to invoke the UDF on
3. Finish - UDF has been invoked on all available data

### PySpark -> Spark

1. Response data
2. Exceptions
3. Finish - All processing is done
