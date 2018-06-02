from datetime import datetime, timezone
import time
import pytz

def tsToDt(timestamp, str=False):
    """Converts an integer or float timestamp into a datetime object (UTC).
    Give the optional parameter str=True to have it return a date string instead of a datetime object."""
    if type(timestamp) == int or type(timestamp) == float:
        dt = datetime.utcfromtimestamp(int(timestamp))
        if str:
            return dtToStr(dt)
        else:
            return dt
    else:
        raise RuntimeError("Timestamp provided must be int or float.")
        
def dtToTs(dt):
    """Converts a datetime.datetime class object OR a human-readable datetime string 
    into an integer timestamp. The datetime string must have the following format: %Y-%m-%d %H:%M:%S"""
    if isinstance(dt, datetime):
        ts = dt.replace(tzinfo=timezone.utc).timestamp()
        return int(ts)
    elif isinstance(dt, str):
        utc = pytz.utc
        ts = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        utc_ts = int(utc.localize(ts, is_dst=None).timestamp())  
        return utc_ts
    else:
        raise RuntimeError("Datetime provided must be either a datetime.datetime class object or str.")
        
        
def dtToStr(dt):
    """Converts a datetime object into a human readable string."""
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        raise RuntimeError("Datetime provided must be a datetime.datetime class object.")
        
        
        
        
def selftest():
    """Self test for the converters."""
    now_dt = datetime.utcnow()
    print("Datetime object: \t\t\t\t{}".format(now_dt))
    
    #Turn it into humanreadable datetime string
    print("Converting datetime obj into datetime str")
    now_dt_readable = dtToStr(now_dt)
    print("Human-readable datetime string: \t\t{}".format(now_dt_readable))
    
    #Turn str into timestamp
    print("Converting datetime str into timestamp.")
    now_ts = dtToTs(now_dt_readable)
    print("Timestamp from str: \t\t\t\t{}".format(now_ts))
    
    #turn datetime obj into a timestamp
    print("Converting datetime obj into timestamp.")
    now_ts = dtToTs(now_dt)
    print("Timestamp: \t\t\t\t\t{}".format(now_ts))
    
    #turn it back into a datetime object
    print("Converting timestamp into datetime object.")
    now_dt = tsToDt(now_ts)
    print("Datetime: \t\t\t\t\t{}".format(now_dt))    