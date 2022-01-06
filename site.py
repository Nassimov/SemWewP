"""
Site web with Flask
"""

import rdflib
from flask import Flask,render_template,request,redirect

app = Flask(__name__)

# ok
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/sensorroom')
def sensorroom():
    return render_template("sensorroom.html")

@app.route('/observation')
def observation():
    return render_template("observation.html")

# http://127.0.0.1:3030/sensor?id_sensor=03f5ca58_aa70_47b3_980c_c8f486cac9ee
@app.route('/sensor')
def sensor():
    id_sensor=request.args.get('sensor', default = '*', type = str)
    with open('generatedFile.ttl','r') as f:
        for line in f:
            if id_sensor in line:
                print(line)
                #for word in line.split():
                #    print(word) 
    return ;

# http://127.0.0.1:3030/sensor/03f5ca58_aa70_47b3_980c_c8f486cac9ee
# doesnt work
@app.route('/sensor/<id_sensor>')
def sensorId():
    #id_sensor=request.args.get('sensor', default = '*', type = str)
    with open('generatedFile.ttl','r') as f:
        for line in f:
            if id_sensor in line:
                print(line)
                #for word in line.split():
                #    print(word) 
    return ;

@app.route("/index")
def ind():
    return redirect("./territoire.emse.fr/kg/emse/fayol/index.html")
    #return


# page data
@app.route('/data', methods=["POST","GET"])
def data():
    return render_template("data.html",room=request.form.get("room","emse"))

# aff site ext
@app.route('/graph')
def graph():
    graph = rdflib.Graph()
    rdfData = "generatedFile2.ttl"
    g = graph.parse(rdfData)
    #print(g)
    return g


if __name__ == "__main__":
    app.run(host='127.0.0.1',port=3030,debug=True)
