# LegendQL

A Python metamodel that represents a Query to be executed in the context of a Runtime, thereby creating a DataFrame.

## Legend Relational REPL
You can download a shaded executable jar of the legend relational repl [here](https://www.icloud.com/iclouddrive/0b1JqLjnL-htafqLRf2kzg3bg#legend-engine-repl-relational-4.74.1-SNAPSHOT):

You can run the relational runtime by executing: `java -jar legend-engine-repl-relational-4.74.1-SNAPSHOT.jar`

Place the downloaded jar in the root directory of this project.

You should now be able to run tests that rely on the repl.

## Legend Execution Server
You can download the http server jar of the legend execution server [here](https://repo1.maven.org/maven2/org/finos/legend/engine/legend-engine-server-http-server/4.78.3/legend-engine-server-http-server-4.78.3-shaded.jar)

From the root directory of this project, you can run the execution server by executing: `java -jar test/executionserver/legend-engine-server-http-server-4.78.3-shaded.jar server test/executionserver/userTestConfig.json`

Place the downloaded jar in test/executionserver

You should now be able to run tests that rely on the execution server
