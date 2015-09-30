# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify
import os, csv
import pandas as pd
from analysis import query_stats

app = Flask(__name__)
app.config.update(dict(DEBUG=True))

@app.route('/')
def home():
    result = query_stats("2013-08-01", "2013-08-02")
    return render_template('index.html', stats=result)

@app.route('/stats', methods=['POST'])
def home_post():
    startdate = request.form['startdate']
    enddate = request.form['enddate']
    result = query_stats(startdate, enddate)
    return render_template('index.html', stats=result)

if __name__ == '__main__':
    app.run()
