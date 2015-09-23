import pandas as pd
from pandas import datetime, Timedelta
import urllib2
import os
import cPickle as pickle

class TurnstileDataLoader:
    """Automatically load data from MTA website"""
    
    def __init__(self):
        # format of links to the txt files
        self.url_base = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_{0}.txt"
        # first day of data
        self.begining_of_time = datetime(2010, 5, 1)
        # date when format of data changed
        self.new_era = datetime(2014, 10, 18)
        self.today = datetime.today()
        
        # prepare station df for old format data
        self.data_dir = "static/data/"
        station_df_path = os.path.join(self.data_dir, "station.pkl")
        if os.path.isfile(station_df_path):
            with open(station_df_path) as f:
                self.station_df = pickle.load(f)
        else:
            self.station_df = pd.read_excel("http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls")
            self.station_df.columns = ["UNIT", "C/A", "STATION", "LINENAME", "DIVISION"]
            # save to data directory
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
            with open(station_df_path, "wb") as f:
                pickle.dump(self.station_df, f)
                    
    def _next_saturday(self, date):
        """Find the nearest saturday after input date when data updated"""
        weekday = date.weekday()
        delta = Timedelta(7 - (weekday + 2) % 7, unit='d')
        return date + delta
    
    def _find_files(self, start_date, end_date):
        """Find list of files covering specified starting date and 
        end date."""
        # some assertions
        assert(start_date < end_date)
        assert(start_date >= self.begining_of_time)
        # assert data is available on mta website
        assert(self._next_saturday(end_date) <= self.today)
        
        files = []
        start_saturday = self._next_saturday(start_date)
        end_saturday = self._next_saturday(end_date)
        while start_saturday <= end_saturday:
            datestr = start_saturday.strftime("%y%m%d")
            files.append(self.url_base.format(datestr))
            start_saturday += Timedelta("7 days")
        return files
        
    def _load_old(self, txtfileurl):
        """Load old format txt file which needs quite some reformating"""
        records = []
        txtfile = urllib2.urlopen(txtfileurl)
        for line in txtfile:
            row = line.strip().split(",")
            if len(row) < 8:
                continue
            ca, unit, scp = row[:3]
            i = 3
            while i < len(row):
                date, time, desc, entries, exits = row[i:i+5]
                date_time = datetime.strptime(date + " " + time, "%m-%d-%y %H:%M:%S")
                record = dict(DATE_TIME=date_time, UNIT=unit, SCP=scp,
                              DESC=desc, ENTRIES=int(entries), EXITS=int(exits))
                record["C/A"] = ca
                records.append(record)
                i += 5
        old_df = pd.DataFrame.from_records(records)
        return pd.merge(old_df, self.station_df, how="left").set_index(["DATE_TIME"])

    def load(self, txtfileurl):
        """Load txt file specified by a url as a DataFrame"""
        datestr = txtfileurl[-10:-4]
        # check if we have the data saved already
        filepath = os.path.join(self.data_dir, datestr + ".pkl")
        if os.path.isfile(filepath):
            with open(filepath) as f:
                result = pickle.load(f)
        else:
            # detect whether this file is of the newer, cleaner format
            is_new = datetime.strptime(datestr, "%y%m%d") >= self.new_era
            if is_new:
                result = pd.read_csv(txtfileurl, parse_dates=[[6,7]], index_col=["DATE_TIME"]).sort_index()
                result.columns = ["C/A", "UNIT", "SCP", "STATION", "LINENAME", 
                                  "DIVISION", "DESC", "ENTRIES", "EXITS"]
            else:
                result = self._load_old(txtfileurl).sort_index()
                
            result["TURNSTILE_ID"] = result[["C/A", "UNIT", "SCP"]].apply(lambda x: " ".join(x), axis=1)
            result.drop(["C/A", "UNIT", "SCP"], axis=1, inplace=True)
            with open(filepath, "wb") as f:
                pickle.dump(result, f)
                
        return result

    def retrieve(self, start_date, end_date):
        """Retrieve data given starting date and end date.
        Note that starting date is included while end date is not.
        """
        files = self._find_files(start_date, end_date)
        frames = []
        for fileurl in files:
            frames.append(self.load(fileurl))
        result = pd.concat(frames)
        # need to include the first entry of end date to calculate count
        # the latest first entry on any day is 3am
        end_time = end_date.replace(hour=3, minute=0, second=0, microsecond=0)
        return result[start_date:end_time]
