from datetime import datetime
import pandas as pd
from rdflib import Namespace
from rdflib.namespace import CSVW, DC, DCAT, DCTERMS, DOAP, FOAF, ODRL2, ORG, OWL, \
    PROF, PROV, RDF, RDFS, SDO, SH, SKOS, SOSA, SSN, TIME, \
    VOID, XMLNS, XSD
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, Literal
from rdflib import Literal
import tarfile
from tableMeteoExtractor import TableMeteoExtractor
from SPARQLWrapper import SPARQLWrapper, CSV
from string import Template
from io import StringIO
from time import sleep
class Main():
    def __init__(self):
        self.sensorsData = None
        self.orderedSensorData = None
        self.queryEndpoint = 'http://localhost:3030/semwebdb/query'
        self.updateEndpoint = 'http://localhost:3030/semwebdb/update'
        self.sparqlEndpont=SPARQLWrapper("http://localhost:3030/semwebdb/sparql")
        self.store = sparqlstore.SPARQLUpdateStore()
        self.rdfGraph = Graph()
        self.currentComputedSensor = None
        self.currentComputedRoom = None

        # NameSpaces
        self.ROOM = Namespace("https://territoire.emse.fr/kg/emse/fayol/4ET/")
        self.SENSOR = Namespace("http://localhost:3030/sensor/")
        self.OBS = Namespace("http://localhost:3030/observation/")
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
            _temperatureObs = self.OBS["{}_temp_id_{}".format(
                self.currentComputedSensor, idRow)]
            self.rdfGraph.add(
                (_temperatureObs, RDF.type, SOSA.Observation))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _temperatureObs))
            self.rdfGraph.add((_temperatureObs, SOSA.observedProperty,
                              self.ROOM["{}#temperature".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_temperatureObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_temperatureObs, SOSA.hasSimpleResult, Literal(
                str(float(row["TEMP"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------------------
        # if sensor don't capture humidity we don't even connecte it to room humidity , event if we could do it but we will not get any result.

        if row["HMDT"]:
            _humidityObs = self.OBS["{}_hmdt_id_{}".format(
                self.currentComputedSensor, idRow)]
            self.rdfGraph.add(
                (_humidityObs, RDF.type, SOSA.Observation))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _humidityObs))
            self.rdfGraph.add((_humidityObs, SOSA.observedProperty,
                              self.ROOM["{}#humidity".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_humidityObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_humidityObs, SOSA.hasSimpleResult, Literal(
                str(float(row["HMDT"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------------------
        # if sensor don't capture luminosity we don't even connecte it to room luminosity , event if we could do it but we will not get any result.
        if row["LUMI"]:
            _luminosityObs = self.OBS["{}_lumi_id_{}".format(
                self.currentComputedSensor, idRow)]
            self.rdfGraph.add(
                (_luminosityObs, RDF.type, SOSA.Observation))
            self.rdfGraph.add(
                (self.SENSOR[self.currentComputedSensor], SSN.detects, _luminosityObs))
            self.rdfGraph.add((_luminosityObs, SOSA.observedProperty,
                              self.ROOM["{}#luminosity".format(self.currentComputedRoom)]))
            self.rdfGraph.add((_luminosityObs, SOSA.resultTime,
                              Literal(_datetime, datatype=XSD.datetime)))
            self.rdfGraph.add((_luminosityObs, SOSA.hasSimpleResult, Literal(
                str(float(row["LUMI"])), datatype=XSD.float)))

        # --------------------------------------------------------------------------------------------------------------------------

    def extractSensorsInformations(self):
        """Extract informations from CSV and Connect sensors to 4th floor sensors"""

        # --------------------INIT-------------------
        self.__extracTars__(archives="./sensorsData.tar.gz")
        self.sensorsData = pd.read_csv(
            './sensorsData/20211116-daily-sensor-measures.csv')
        # BIND--------------------------------
        self.rdfGraph.bind("room", self.ROOM)
        self.rdfGraph.bind("sensor", self.SENSOR)
        self.rdfGraph.bind("core", self.CORE)
        self.rdfGraph.bind("seas", self.SEAS)
        self.rdfGraph.bind("sosa", SOSA)
        self.rdfGraph.bind("ssn", SSN)
        self.rdfGraph.bind("rdf", RDF)
        self.rdfGraph.bind("xsd", XSD)
        self.rdfGraph.bind("observation", self.OBS)
        # ---------------------------------------
        _masque = self.sensorsData['location'].str.contains(
            'emse/fayol/e4/S4\w+')
        self.sensorsData = self.sensorsData[_masque]
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
                self.currentComputedSensor = self.currentComputedSensor.replace(
                    "-", "_")
                _sensor = str(_row["location"])
                self.currentComputedRoom = _sensor.split("/")
                if len(self.currentComputedRoom) == 4 and self.currentComputedRoom[3][0:2] == "S4":
                    self.currentComputedRoom = self.currentComputedRoom[3][1:]
                    # Relation Room to Sensor , we were hesitating between isLocationOf and hosts and we think that they are semantically equivalent
                    self.rdfGraph.add(
                        (self.SENSOR[self.currentComputedSensor], RDF.type, SOSA.Sensor))
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

    def __getRoomAndTimeWhereTemperatureDifferenceFromOutsideIsLarge__(self, day, meteoData):
        """generate RDF FILE  of HTML TABLE OF Meteo france 15/11/2021 and 16/11/2021"""


        _delta =float(2)
        for _index, _row in meteoData.iterrows():

            _hour = int(_row["hour"])
            _temp = float(_row["temperature"])
            _d1=datetime(year=2021, month=11, day=day, hour=_hour).strftime("%Y-%m-%dT%H:%M:%S")

            _d2=datetime(year=2021, month=11, day=day, hour=_hour,minute=59, second=59).strftime("%Y-%m-%dT%H:%M:%S")
            if _temp <= 0:
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             xsd:float(?result) >= $v1  && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
                )

                self.sparqlEndpont.setQuery(_query.substitute(v1=25,d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure( day= day, temp= _temp, d1= _d1, d2= _d2, meteoType= "alarmantByGroup" , path="Alarming"  )
            elif _temp > 0 and _temp <=5:
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             (xsd:float(?result) >= $v1 && xsd:float(?result) < $v2 )  && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
                    )

                self.sparqlEndpont.setQuery(_query.substitute(v1=22,v2=25,d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp, d1= _d1, d2= _d2, meteoType= "interessantByGroup" , path="OfInterest" )
            elif _temp >5 and _temp <=10 : 
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             (xsd:float(?result) >= $v1 && xsd:float(?result) < $v2 ) && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(20),v2=float(22),d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "normalByGroup" , path= "Normal" )
                
                
            if _temp <=10 :
                # by inter-ranges
                       
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             xsd:float(?result) >= $v1  && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(25),d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "Alarming-InterRanges" , path= "Alarming" )    

                #---------------------------------------------------------------------------------------------------------------------
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             (xsd:float(?result) >= $v1 && xsd:float(?result) < $v2 ) && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(22) , v2=  float(25) ,d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "Normal-InterRanges" , path= "Normal" ) 
            elif _temp > 10 and _temp <=20 :
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             xsd:float(?result) >= $v1  && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(25),d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "Alarming-InterRanges" , path= "Alarming" )    

                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             (xsd:float(?result) >= $v1 && xsd:float(?result) < $v2  ) && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(20), v2= float(23) ,d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "Normal-InterRanges" , path= "Normal" )    

                
                _query = Template(
                """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX sensor: <http://localhost:3030/sensor/>
                PREFIX sosa: <http://www.w3.org/ns/sosa/>
                PREFIX room: <https://territoire.emse.fr/kg/emse/fayol/4ET/>
                PREFIX core: <https://w3id.org/rec/core/>
                PREFIX ssn: <http://www.w3.org/ns/ssn/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?room ?datetime ?result
                            WHERE {
                            ?room core:isLocationOf ?sensor .
                            ?sensor ssn:detects ?observation .
                            ?observation sosa:hasSimpleResult ?result ; sosa:resultTime ?datetime ; sosa:observedProperty ?prop .
                            FILTER (
                             (xsd:float(?result) >= $v1 && xsd:float(?result) < $v2  ) && (?datetime > "$d1"^^xsd:dateTime && ?datetime < "$d2"^^xsd:dateTime) &&  regex(str(?prop),"temperature")   
                            )
                                        
                            }
                
                
                
                """
            )

                self.sparqlEndpont.setQuery(_query.substitute(v1=float(23), v2= float(25) ,d1=_d1 ,d2= _d2 ))
                self.generateCsvGroupMesure(day= day, temp= _temp,d1= _d1, d2= _d2, meteoType= "OfInterest-InterRanges" , path= "OfInterest" )               
            

    def generateCsvGroupMesure(self, day, temp, d1, d2, meteoType, path):
        self.sparqlEndpont.setReturnFormat(CSV)
        _results = self.sparqlEndpont.query().convert()

        _bData = StringIO(_results.decode("utf-8"))

        df = pd.read_csv(_bData, sep=",")
        if not df.empty :
            df.to_csv("./ResultClassification/{}/day_{}_from_{}_to_{}_outsideTemp_{}_{}.csv".format(path,day,d1,d2,temp, meteoType))

    def manageTemperatureDifferenceFromOutside(self):
        """Generate csv files of Room and time where outside temperature is very differente(Delta -> +/- 2) from sensor observation"""
        _extractor = TableMeteoExtractor(
            url15Nov="https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=15&mois2=10&annee2=2021",
            url16Nov="https://www.meteociel.fr/temps-reel/obs_villes.php?code2=7475&jour2=16&mois2=10&annee2=2021"
        )
        _extractor.run()
        sleep(5)
        self.__getRoomAndTimeWhereTemperatureDifferenceFromOutsideIsLarge__(day=15,meteoData=_extractor.data15Nov2021)
        self.__getRoomAndTimeWhereTemperatureDifferenceFromOutsideIsLarge__(day=16,meteoData=_extractor.data16Nov2021)

test = Main()
test.manageTemperatureDifferenceFromOutside()
