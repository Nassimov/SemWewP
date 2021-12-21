from datetime import datetime
import pandas as pd
from rdflib import Namespace, plugin, Dataset
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
                           PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME, \
                           VOID, XMLNS, XSD
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.store import Store
import rdflib_sqlalchemy
from rdflib import ConjunctiveGraph, Literal
import rdflib
from rdflib import plugin
import numpy as np
import uuid
class Main():
    def __init__(self):
        self.sensorsData = pd.read_csv('./sensorsData/20211116-daily-sensor-measures.csv')
        self.orderedSensorData=None
        self.queryEndpoint='http://localhost:3030/test/query'
        self.updateEndpoint='http://localhost:3030/test/update'
        self.store = sparqlstore.SPARQLUpdateStore()
        self.rdfGraph = Graph()
        self.currentComputedSensor=None
        self.currentComputedRoom=None
            
        # NameSpaces
        self.ROOM=Namespace("https://territoire.emse.fr/kg/emse/fayol/4ET/")
        self.SENSOR = Namespace("http://localhost:3030/sensor/") 
        self.CORE= Namespace("https://w3id.org/rec/core/")
        self.SEAS=Namespace("https://w3id.org/seas/")   
    
    def manageSensorObservation(self, row):
        """Connect current sensor to its observation"""
        _timestmp= int(row["time"])/10**9
        _datetime=datetime.fromtimestamp(_timestmp)
        _datetime = _datetime.strftime("%Y-%m-%dT%H:%M:%S")
        # We connect sensor to room  observations
        # --------------------------------------------------------------------------------------------------------------------------
        #if sensor don't capture temperature we don't even connecte it to room temperature , event if we could do it but we will not get any result.

        if row["TEMP"]:
            _newTemperature= BNode(uuid.uuid4())
            self.rdfGraph.add(  ( self.SENSOR[self.currentComputedSensor] , SSN.detects , _newTemperature ) )
            self.rdfGraph.add(  ( _newTemperature , SSN.hasProperty , self.ROOM["{}#temperature".format(self.currentComputedRoom)] ) )
            self.rdfGraph.add(  ( _newTemperature , SOSA.resultTime , Literal(_datetime, datatype=XSD.datetime ) ) )
            self.rdfGraph.add(  ( _newTemperature , SOSA.hasSimpleResult , Literal(str(float(row["TEMP"])), datatype=XSD.float ) ) )

                
            
        # --------------------------------------------------------------------------------------------------------------------------
        
        # --------------------------------------------------------------------------------------------------------------------------
        #if sensor don't capture humidity we don't even connecte it to room humidity , event if we could do it but we will not get any result.

        if row["HMDT"]:
            _newHumidity= BNode(uuid.uuid4())
            self.rdfGraph.add(  ( self.SENSOR[self.currentComputedSensor] , SSN.detects , _newHumidity ) )
            self.rdfGraph.add(  ( _newHumidity , SSN.hasProperty , self.ROOM["{}#humidity".format(self.currentComputedRoom)] ) )
            self.rdfGraph.add(  ( _newHumidity , SOSA.resultTime , Literal(_datetime, datatype=XSD.datetime ) ) )
            self.rdfGraph.add(  ( _newHumidity , SOSA.hasSimpleResult , Literal(str(float(row["HMDT"])), datatype=XSD.float ) ) )

                

        # --------------------------------------------------------------------------------------------------------------------------
        
        
        # --------------------------------------------------------------------------------------------------------------------------
        #if sensor don't capture luminosity we don't even connecte it to room luminosity , event if we could do it but we will not get any result.
        if row["LUMI"]:
            _newLuminosity= BNode(uuid.uuid4())
            self.rdfGraph.add(  ( self.SENSOR[self.currentComputedSensor] , SSN.detects , _newLuminosity ) )
            self.rdfGraph.add(  ( _newLuminosity , SSN.hasProperty , self.ROOM["{}#luminosity".format(self.currentComputedRoom)] ) )
            self.rdfGraph.add(  ( _newLuminosity , SOSA.resultTime , Literal(_datetime, datatype=XSD.datetime ) ) )
            self.rdfGraph.add(  ( _newLuminosity, SOSA.hasSimpleResult , Literal(str(float(row["LUMI"])), datatype=XSD.float ) ) )

                

        # --------------------------------------------------------------------------------------------------------------------------          
    
    def extractSensorsInformations(self):
        """Extract informations from CSV and Connect sensors to 4th floor sensors"""
        #BIND--------------------------------
        self.rdfGraph.bind("room",self.ROOM)
        self.rdfGraph.bind("sensor",self.SENSOR)
        self.rdfGraph.bind("core",self.CORE)
        self.rdfGraph.bind("seas",self.SEAS)
        #---------------------------------------
        _masque=self.sensorsData['location'].str.contains('emse/fayol/e4/S4\w+')
        print(len(self.sensorsData))
        self.sensorsData=self.sensorsData[_masque]
        print(len(self.sensorsData))
        self.orderedSensorData=self.sensorsData.sort_values(by=["id"])
        #Change all "nan" to None 
        self.orderedSensorData=self.orderedSensorData.where(pd.notnull(self.orderedSensorData),None)
        
        for _i, _row in self.orderedSensorData.iterrows():
            if self.currentComputedSensor == _row["id"] :
                # Add sensor captured temperature
                self.manageSensorObservation(row=_row)
                
            else:
                # relation betweene room and the new sensor
                self.currentComputedSensor =str(_row["id"])
                _sensor=str(_row["location"])
                self.currentComputedRoom= _sensor.split("/")
                if len(self.currentComputedRoom) == 4 and self.currentComputedRoom[3][0:2]=="S4":
                    self.currentComputedRoom=self.currentComputedRoom[3][1:]
                    # Relation Room to Sensor , we were hesitating between isLocationOf and hosts and we think that they are semantically equivalent
                    self.rdfGraph.add(  (  self.ROOM[ self.currentComputedRoom ] , self.CORE.isLocationOf , self.SENSOR[self.currentComputedSensor] )  )
                    # add sensor captured temperature...
                    self.manageSensorObservation(row=_row)
                else:
                    # <don't take into account sensors if the space isn't a room>
                    
                    self.currentComputedSensor=None

        
        self.rdfGraph.serialize(destination="generatedFile.ttl")
    
    def insertionSensorDataGraph(self):
        """
        Insert to BDD the converted Non rdf datas 
        we uploaded directly the generated file to the playstore but  we can also use this methode
        """
        self.store.open((self.queryEndpoint, self.updateEndpoint))
        #! you have first to untar the tar file
        self.rdfGraph.parse("./generatedFile.ttl")
        self.store.add_graph(self.rdfGraph)
        self.store.close()
        
test=Main()
test.extractSensorsInformations()        
    
    
        
        
        
        