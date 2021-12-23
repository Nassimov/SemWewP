from rdflib.plugins.sparql import prepareQuery

q = prepareQuery(
        """SELECT ?room ?datetime ?result

            WHERE {?room core:isLocationOf ?sensor .
            ?sensor ssn:detects ?observation .
            ?observation sosa:hasSimpleResult ?result ;
                        sosa:resultTime ?datetime ;
                        ssn:hasProperty ?prop .

            FILTER (
            (xsd:float(?result) < ?var || xsd:float(?result) > 21 ) && (?datetime > "2021-11-16T00:00:00"^^xsd:dateTime && ?datetime < "2021-11-16T23:59:59"^^xsd:dateTime) &&  regex(str(?prop),"temperature")
            )

            }
            LIMIT 25""",
        initNs={"rdf": RDF,
        "rdfs": RDFS,
 "sensor": Namespace("http://localhost:3030/sensor/%22"),
 "sosa": SOSA,
 "room": Namespace("https://territoire.emse.fr/kg/emse/fayol/4ET/%22"),
 "core":Namespace("https://w3id.org/rec/core/%22"),
 "ssn": SSN,
 "xsd": XSD},
    )
    for row in store.query(q, initBindings={"var": 20.0  }):
        print(row)