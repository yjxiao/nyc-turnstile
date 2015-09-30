from data_loader import TurnstileDataLoader
from datetime import datetime

def get_valid_readings(start_df, end_df):
    setA = set()
    setB = set()
    for _, row in start_df.iterrows():
        setA.add(row["TURNSTILE_ID"])
    for _, row in end_df.iterrows():
        setB.add(row["TURNSTILE_ID"])
    valid_set = setA.intersection(setB)
    bad_set = set()
    for tid in valid_set:
        rowA = start_df[start_df["TURNSTILE_ID"] == tid]
        rowB = end_df[end_df["TURNSTILE_ID"] == tid]
        if (rowA.ENTRIES.sum() > rowB.ENTRIES.sum()) |\
           (rowA.EXITS.sum() > rowB.EXITS.sum()):
            bad_set.add(tid)
    valid_set -= bad_set
    maskA = start_df.apply(lambda x: x["TURNSTILE_ID"] in valid_set, axis=1)
    maskB = end_df.apply(lambda x: x["TURNSTILE_ID"] in valid_set, axis=1)
    return maskA, maskB

def query_stats(startdate, enddate):
    dataloader = TurnstileDataLoader()
    start = datetime.strptime(startdate, "%Y-%m-%d")
    end = datetime.strptime(enddate, "%Y-%m-%d")
    df = dataloader.retrieve(start, end)
    df = df[df.DESC == "REGULAR"]
    start_count = df.ix[start][["TURNSTILE_ID", "STATION", "ENTRIES", "EXITS"]]
    end_count = df.ix[end][["TURNSTILE_ID", "STATION", "ENTRIES", "EXITS"]]
    maskA, maskB = get_valid_readings(start_count, end_count)
    station_countA = start_count[maskA].groupby("STATION").sum()
    station_countB = end_count[maskB].groupby("STATION").sum()
    station_traffic = station_countB - station_countA
    station_traffic["TOTAL"] = station_traffic.ENTRIES + station_traffic.EXITS
    station_traffic.sort("TOTAL", ascending=False, inplace=True)
    total_traffic = station_traffic.TOTAL.sum()
    results = {}
    results["start"] = startdate
    results["end"] = enddate
    results["total"] = "{0:,}".format(total_traffic)
    top10 = station_traffic[:10]
    records = []
    for idx in top10.index:
        entries = "{0:,}".format(top10.ENTRIES.ix[idx].sum())
        exits = "{0:,}".format(top10.EXITS.ix[idx].sum())
        records.append({"station":idx, "entries":entries, "exits":exits})
    results["records"] = records
    return results
