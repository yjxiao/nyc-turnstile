# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
import os, csv
import pandas as pd

app = Flask(__name__)
app.config.update(dict(DEBUG=True))

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def home_post():
    startdate = request.form['startdate']
    enddate = request.form['enddate']
    return "{}".format(type(startdate))

if __name__ == '__main__':
    app.run()
