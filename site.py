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

@app.route("/index")
def ind():
    return redirect("./territoire.emse.fr/kg/emse/fayol/index.html")

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
