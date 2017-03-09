from spyre import server

import pandas as pd
from urllib2 import urlopen
import json
import os
import yaml
import psycopg2
import pandas.io.sql as psql
import seaborn as sns


class Waze(server.App):
    title = "Waze Traffic Notification Alerts Data"

    inputs = [{     "type":'dropdown',
                    "label": 'Date', 
                    "options" : [ {"label": "2017-03-02", "value":"2017-03-02"},
                                  {"label": "2017-03-03", "value":"2017-03-03"},
                                  {"label": "2017-03-04", "value":"2017-03-04"},
                                  {"label": "2017-03-05", "value":"2017-03-05"},
                                  {"label": "2017-03-06", "value":"2017-03-06"},
                                  {"label": "2017-03-07", "value":"2017-03-07"},
                                  {"label": "2017-03-08", "value":"2017-03-08"},
                                  {"label": "2017-03-09", "value":"2017-03-09"}],
                    "key": 'ticker', 
                    "action_id": "update_data"},
                    {"type":'dropdown',
                    "label": 'Type', 
                    "options" : [ {"label": "POLICE", "value":"POLICE"},
                                  {"label": "CHIT CHAT", "value":"CHIT_CHAT"},
                                  {"label": "JAM", "value":"JAM"},
                                  {"label": "ACCIDENT", "value":"ACCIDENT"},
                                  {"label": "HAZARD", "value":"HAZARD"},
                                  {"label": "ROAD CLOSED", "value":"ROAD_CLOSED"}],
                    "key": 'ticker1', 
                    "action_id": "update_data"},]

    controls = [{   "type" : "hidden",
                    "id" : "update_data"}]

    tabs = ["Plot", "Table"]

    outputs = [{ "type" : "plot",
                    "id" : "plot",
                    "control_id" : "update_data",
                    "tab" : "Plot"},
                { "type" : "table",
                    "id" : "table_id",
                    "control_id" : "update_data",
                    "tab" : "Table",
                    "on_page_load" : True }]
    
    def getData(self, params):
      ticker = params['ticker']
      ticker1 = params['ticker1']

      rds_credentials = yaml.load(open(os.path.expanduser('~/postgres_cred.yml')))
      DB_NAME = rds_credentials['waze_postgres']['database']
      DB_user = rds_credentials['waze_postgres']['user']
      DB_password = rds_credentials['waze_postgres']['password']
      DB_host = rds_credentials['waze_postgres']['host']
      DB_port = rds_credentials['waze_postgres']['port']

      # connect to database
      try:
        conn = psycopg2.connect(database=DB_NAME, user=DB_user, password=DB_password, host=DB_host, port=DB_port)
        print "Opened database successfully"

      except:
        print "Unable to connect to database"

      stmnt = "SELECT * FROM event_subtype WHERE time_stamp = '{}' AND type = '{}'".format(ticker,ticker1)

      event_type_df = pd.read_sql(stmnt, conn)
      print(event_type_df.head())

      conn.close()
      return event_type_df

    def getPlot(self, params):
      df = self.getData(params)
      plt_obj = sns.barplot(y="subtype", x="count", data=df) #df.plot.bar(x='type', y='count')
      #plt_obj.set_ylabel("Count")
      plt_obj.set_title("EVENT SUBTYPE")
      fig = plt_obj.get_figure()
      return fig

app = Waze()
app.launch(host='0.0.0.0',port=5000)

















