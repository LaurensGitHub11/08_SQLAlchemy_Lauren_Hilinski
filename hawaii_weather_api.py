import datetime as dt
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
# return all available endpoints
def home():
    return (f"Welcome to the Weather API for Hawaii Available Endpoints:<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/'start-date'<br/>"
            f"/api/v1.0/'start-date'/'end-date'<br/>")

@app.route("/api/v1.0/precipitation")
# convert the query reulsts to a dictoinary using date as the key and prcp as the value
# return the json representation of your dictionary
def precipitation():
    year_prcp = (session.query(Measurement.prcp, Measurement.date)
             .order_by(Measurement.date)
             .all())
    year_prcp_df = pd.DataFrame(year_prcp).dropna().set_index("date")
    precip_dict = year_prcp_df.to_dict()
    Session.close(session)
    return jsonify(precip_dict)

@app.route("/api/v1.0/stations")
# return a json list of stations from the dataset
def stations():
    most_active = session.query(Measurement.station).group_by(Measurement.station).all()
    most_active_df = pd.DataFrame(most_active)
    stations_dict = most_active_df.to_dict()
    Session.close(session)
    return jsonify(stations_dict) 

@app.route("/api/v1.0/tobs")
# query for the dates and temperature observations from a year from the last data point
# return a json list of the tobs for the previous year
def tobs():
    most_recent_date = session.query(func.max(Measurement.date)).all()[0][0]
    most_recent_date_formatted = dt.datetime.strptime(most_recent_date, "%Y-%m-%d")
    year_ago = most_recent_date_formatted - dt.timedelta(days=365)

    year_temp = (session.query(Measurement.tobs, Measurement.date)
             .filter(Measurement.date > year_ago)
             .order_by(Measurement.date)
             .all())
    year_temp_df = pd.DataFrame(year_temp).dropna().set_index("date")
    tobs_dict = year_temp_df.to_dict()
    Session.close(session)
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>")
# return a json list of the min temp, avg temp, max temp for all dates greater than and equal to the start date
def start_date(start):
    min_to_max_start_only = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
        .filter(Measurement.date >= start)\
        .all()
    min_to_max_start_only_dict = {
        "date": start,
        "min_temp": min_to_max_start_only[0][0],
        "avg_temp": min_to_max_start_only[0][1],
        "max_temp": min_to_max_start_only[0][2]
    }
    Session.close(session)
    return jsonify(min_to_max_start_only_dict)

@app.route("/api/v1.0/<start>/<end>")
# return a json list of the min temp, avg temp, max temp for dates between the start and end date inclusive
def start_end_date(start, end):
    min_to_max_start_end = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
        .filter(Measurement.date >= start)\
        .filter(Measurement.date <= end)\
        .all()
    min_to_max_start_end_dict = {
        "date_range": f"{start} - {end}",
        "min_temp": min_to_max_start_end[0][0],
        "avg_temp": min_to_max_start_end[0][1],
        "max_temp": min_to_max_start_end[0][2]
    }
    Session.close(session)
    return jsonify(min_to_max_start_end_dict)   

if __name__ == "__main__":
    app.run(debug=True)