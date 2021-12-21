import pandas as pd
from rdflib import Namespace, plugin, Dataset
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
                           PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME, \
                           VOID, XMLNS, XSD 
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.store import Store
import rdflib_sqlalchemy
from rdflib import ConjunctiveGraph, Namespace, Literal
import rdflib
from rdflib import plugin

from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores import sparqlstore

query_endpoint = 'http://localhost:3030/nassim/query'
update_endpoint = 'http://localhost:3030/nassim/update'
store = sparqlstore.SPARQLUpdateStore()
store.open((query_endpoint, update_endpoint))
g = Graph()
# g.bind("foaf", FOAF)

# bob = URIRef("http://example.org/people/Bob")
# linda =  URIRef("http://example.org/people/Linda")  # a GUID is generated

# name = Literal("Bob")
# age = Literal(24)

# g.add((bob, RDF.type, FOAF.Person))
# g.add((bob, FOAF.name, name))
# g.add((bob, FOAF.age, age))
# g.add((bob, FOAF.knows, linda))
# g.add((linda, RDF.type, FOAF.Person))
# g.add((linda, FOAF.name, Literal("Linda")))
g = Graph(identifier = URIRef('http://www.example.com/'))
g.parse("./territoire.emse.fr/kg/emse/index.n3")

print(g.serialize(format="turtle"))
store.add_graph(g)
# ee=store.query(
#    """


# SELECT ?subject ?predicate ?object
# WHERE {
#   ?subject ?predicate ?object
# }
# LIMIT 25
   
   
   
#    """
# )

# _query="""PREFIX dc: <http://purl.org/dc/elements/1.1/>

# INSERT DATA
# { 
#   <http://example/book0> dc:title "A default book" } """

# store.update(_query)

store.close()
# print(list(ee))
# for l in ee:
#    i,ii,iii=l
#    print(i)
#    print(ii)
#    print(iii)
#    print("------------------------------")
   
