# Nifi ConvertAvroToClassicJSON

No matter what options are selected within [Apache NiFi's](https://nifi.apache.org/) [ConvertAvroToJSON](http://localhost:8080/nifi-docs/documentation?select=org.apache.nifi.processors.avro.ConvertAvroToJSON&group=org.apache.nifi&artifact=nifi-avro-nar&version=1.6.0) it will create JSON which does not reflect what has been received from the Avro upstream, see [issue 5093](https://issues.apache.org/jira/browse/NIFI-5093).

This is a drop in replacement for ConvertAvroToJSON which includes only one option instead of two which controls whether to create NDJSON (new line delimited) or JSON (as an array).

The code is here is basically a bastardized version of the ConvertAvroToJSON so should be available under the same license (Apache).

To use build using maven `maven clean install` and drop the `target/*.nar` in the lib directory just below your Apache NiFi installation directory.
