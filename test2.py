import pandas as pd
from rdflib import Namespace
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
                           PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME, \
                           VOID, XMLNS, XSD 

from rdflib import Graph, URIRef, Literal, BNode

g = Graph()
ex = Namespace("http://www.example.com/")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("ex",ex)
g.bind("rdfs", RDFS)
g.bind("xsd", XSD)
g.bind("geo", geo)


_read=pd.read_csv("./n/stops.csv")
print(len(_read))



p=_read.stop_id[0]
print(type(p))
print(ex)

_length=len(_read)
for _line in range(_length):
    _stop_id=str(_read.stop_id[_line])
    
    g.add(  (   ex[ _stop_id.replace(" ","") ] , RDF.type , geo.SpatialThing  )   )
    g.add(  (   ex[ _stop_id.replace(" ","") ] , RDFS.label , Literal(_read.stop_name[_line], lang="fr")  )   )
    g.add(  (   ex[ _stop_id.replace(" ","")] , geo.lat , Literal(_read.stop_lat[_line], datatype=XSD.decimal)  )   )
    g.add(  (   ex[ _stop_id.replace(" ","") ] , geo.lat , Literal(_read.stop_lon[_line], datatype=XSD.decimal)  )   )
print(g.serialize(format="turtle"))