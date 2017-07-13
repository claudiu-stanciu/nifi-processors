# Nifi Processors

## Build & test
```
mvn clean install
```

## Deploy in Nifi
Copy the nars from /target to `opt/nifi/nifi-<version>/lib`.
Restart Nifi

## ConvertXmlToJson
Convert XML file to JSON
Simple check for valid XML file, without a schema.

# TODO
- add possibility to validate with schema before conversion
