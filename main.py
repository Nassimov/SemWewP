from datetime import datetime
import pandas as pd
from rdflib import Namespace
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
    PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME, \
    VOID, XMLNS, XSD
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, Literal
from rdflib import  Literal
import tarfile

from tableMeteoExtractor import TableMeteoExtractor


class Main():
    def __init__(self):
        self.sensorsData = None
        self.orderedSensorData = None
        self.queryEndpoint = 'http://localhost:3030/test/query'
        self.updateEndpoint = 'http://localhost:3030/test/update'
        self.store = sparqlstore.SPARQLUpdateStore()
        self.rdfGraph = Graph()
        self.currentComputedSensor = None
        self.currentComputedRoom = None

        # NameSpaces
        self.ROOM = Namespace("https://territoire.emse.fr/kg/emse/fayol/4ET/")
        self.SENSOR = Namespace("http://localhost:3030/sensor/")
        self.OBS= Namespace("http://localhost:3030/observation/")
        self.CORE = Namespace("https://w3id.org/rec/core/")
        self.SEAS = Namespace("https://w3id.org/seas/")
    def __extracTars__(self, archives):
        _tar = tarfile.open(archives)
        _tar.extractall('./')
        _tar.close()
    def __manageSensorObservation__(self, row, idRow):
        """Connect current sensor to its observation"""
        _timestmp = int(row["time"])/10**9
        _datetime = datetime.fromtimestamp(_timestmp)
        _datetime = _datetime.strftime("%Y-%m-%dT%H:%M:%S")
        # We connect sensor to room  observations
        # --------------------------------------------------------------------------------------------------------------------------
        # if sensor don't capture temperature we don't even connecte it to room temperature , event if we could do it but we will not get any result.

        if row["TEMP"]:
            _temperatureObs=self.OBS["{}_temp_id_{}".format(self.currentComputedSensor,idRow)]
            self.rdfGraph.add(
                (_temperatureObs , RDF.type, SOSA.Observation ))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _temperatureObs))
            self.rdfGraph.add((_temperatureObs, SSN.hasProperty,
                              self.ROOM["{}#temperature".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_temperatureObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_temperatureObs, SOSA.hasSimpleResult, Literal(
                str(float(row["TEMP"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------------------
        # if sensor don't capture humidity we don't even connecte it to room humidity , event if we could do it but we will not get any result.

        if row["HMDT"]:
            _humidityObs=self.OBS["{}_hmdt_id_{}".format(self.currentComputedSensor,idRow)]
            self.rdfGraph.add(
                (_humidityObs, RDF.type, SOSA.Observation ))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _humidityObs))
            self.rdfGraph.add((_humidityObs, SSN.hasProperty,
                              self.ROOM["{}#humidity".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_humidityObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_humidityObs, SOSA.hasSimpleResult, Literal(
                str(float(row["HMDT"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------------------
        # if sensor don't capture luminosity we don't even connecte it to room luminosity , event if we could do it but we will not get any result.
        if row["LUMI"]:
            _luminosityObs=self.OBS["{}_lumi_id_{}".format(self.currentComputedSensor,idRow)]
            self.rdfGraph.add(
                (_luminosityObs, RDF.type, SOSA.Observation ))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _luminosityObs))
            self.rdfGraph.add((_luminosityObs, SSN.hasProperty,
                              self.ROOM["{}#luminosity".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_luminosityObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_luminosityObs, SOSA.hasSimpleResult, Literal(
                str(float(row["LUMI"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

    def extractSensorsInformations(self):
        """Extract informations from CSV and Connect sensors to 4th floor sensors"""
        
        #--------------------INIT-------------------
        self.__extracTars__(archives="./sensorsData.tar.gz")
        self.sensorsData=pd.read_csv(
            './sensorsData/20211116-daily-sensor-measures.csv')
        # BIND--------------------------------
        self.rdfGraph.bind("room", self.ROOM)
        self.rdfGraph.bind("sensor", self.SENSOR)
        self.rdfGraph.bind("core", self.CORE)
        self.rdfGraph.bind("seas", self.SEAS)
        self.rdfGraph.bind("sosa", SOSA)
        self.rdfGraph.bind("ssn",SSN)
        self.rdfGraph.bind("rdf",RDF)
        self.rdfGraph.bind("xsd",XSD)
        self.rdfGraph.bind("observation",self.OBS)
        # ---------------------------------------
        _masque = self.sensorsData['location'].str.contains(
            'emse/fayol/e4/S4\w+')
        print(len(self.sensorsData))
        self.sensorsData = self.sensorsData[_masque]
        print(len(self.sensorsData))
        self.orderedSensorData = self.sensorsData.sort_values(by=["id"])
        # Change all "nan" to None
        self.orderedSensorData = self.orderedSensorData.where(
            pd.notnull(self.orderedSensorData), None)

        for _i, _row in self.orderedSensorData.iterrows():
            if self.currentComputedSensor == _row["id"]:
                # Add sensor captured temperature
                self.__manageSensorObservation__(row=_row, idRow=_i)

            else:
                # relation betweene room and the new sensor
                self.currentComputedSensor = str(_row["id"])
                self.currentComputedSensor = self.currentComputedSensor.replace("-","_")
                print(self.currentComputedSensor)
                _sensor = str(_row["location"])
                self.currentComputedRoom = _sensor.split("/")
                if len(self.currentComputedRoom) == 4 and self.currentComputedRoom[3][0:2] == "S4":
                    self.currentComputedRoom = self.currentComputedRoom[3][1:]
                    # Relation Room to Sensor , we were hesitating between isLocationOf and hosts and we think that they are semantically equivalent
                    self.rdfGraph.add(
                        (self.SENSOR[self.currentComputedSensor], RDF.type, SOSA.Sensor ))
                    self.rdfGraph.add(
                        (self.ROOM[self.currentComputedRoom], self.CORE.isLocationOf, self.SENSOR[self.currentComputedSensor]))
                    # add sensor captured temperature...
                    self.__manageSensorObservation__(row=_row, idRow=_i)
                else:
                    # <don't take into account sensors if the space isn't a room>

                    self.currentComputedSensor = None

        self.rdfGraph.serialize(destination="generatedFile.ttl")

    def insertionSensorDataGraph(self):
        """
        Insert to BDD the converted Non rdf datas 
        we uploaded directly the generated file to the playstore but  we can also use this methode
        """
        self.store.open((self.queryEndpoint, self.updateEndpoint))
        #! you have first to untar the tar file
        self.__extracTars__(archives="./generatedFile.ttl.tar.gz")
        self.rdfGraph.parse("./generatedFile.ttl")
        self.store.add_graph(self.rdfGraph)
        self.store.close()

    def generateRDFDataMeteo(self):
        """generate RDF FILE  of HTML TABLE OF Meteo france 15/11/2021 and 16/11/2021"""
        _extractor = TableMeteoExtractor(
            url15Nov="https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=15&mois2=10&annee2=2021", 
            url16Nov="https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=16&mois2=10&annee2=2021"
            )
        _extractor.run()
        
        
        # datetime.datetime(year=2021,month=11,day=15,hour=20).strftime("%Y-%m-%dT%H:%M:%S")
        
        
        


test = Main()
test.extractSensorsInformations()  
