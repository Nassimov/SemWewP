"""
Site web with Flask
"""

import rdflib
from flask import Flask,render_template,request

app = Flask(__name__)


@app.route('/')
def index():
    return render_template("index.html")

@app.route('/data')
def data():
    return render_template("data.html",name=request.args.get("name", "world"))

# aff site ext
@app.route("/ind")
def ind():
    return render_template("index.html")

@app.route('/gr')
def graph():
    graph = rdflib.Graph()
    rdfData = "generatedFile.ttl"
    g = graph.parse(data=rdfData,format='rdf')
    print(g)
    return g


if __name__ == "__main__":
    app.run()
