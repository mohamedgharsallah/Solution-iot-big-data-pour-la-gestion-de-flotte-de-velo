# import necessary libraries
import psycopg2
from flask import Flask, jsonify, render_template

# create a Flask app
app = Flask(__name__)


# connect to the PostgreSQL database
conn = psycopg2.connect(user='sara', host='localhost', database='pfadata', password='saroura1', port=5432)
cur = conn.cursor()

# define a Flask route to fetch the avg_duration data from the database
@app.route('/avg_duration')
def get_avg_duration_data():
    cur.execute("SELECT * FROM avg_duration")
    data = cur.fetchall()
    return jsonify(data)

# define a Flask route to fetch the mqtt_msg data from the database
@app.route('/mqtt_msg')
def get_mqtt_msg_data():
    cur.execute("SELECT * FROM mqtt_msg")
    data = cur.fetchall()
    return jsonify(data)

# create a web page using HTML, CSS, and JavaScript to display the data
@app.route('/')
def dashboard():
    return render_template('dashboard.html')





# define a Flask route to fetch the histogram data from the database
@app.route('/histogram')
def get_histogram_data():
    cur.execute("SELECT avg_duration_sec FROM avg_duration")
    data = cur.fetchall()
    durations = [x[0] for x in data if x[0] is not None]
    hist_data = {
        'labels': [],
        'data': []
    }
    for i in range(0, 2000, 100):
        count = len([x for x in durations if x >= i and x < i + 100])
        hist_data['labels'].append(f'{i}-{i+100}')
        hist_data['data'].append(count)
    return jsonify(hist_data)

@app.route('/popular_station')
def get_popular_station_data():
    cur.execute("SELECT start_station_name, end_station_name, COUNT(*) as count FROM popular_station GROUP BY start_station_name, end_station_name ORDER BY count DESC LIMIT 10")
    rows = cur.fetchall()
    data = []
    for row in rows:
        data.append({
            'start_station_name': row[0],
            'end_station_name': row[1],
            'count': row[2]
        })

    return jsonify(data)


@app.route('/popular_bikes')
def get_popular_bikes_data():
    cur.execute("SELECT bike_id, count FROM popular_bikes")
    rows = cur.fetchall()
    data = []
    for row in rows:
        data.append({
            'bike_id': row[0],
            'count': row[1]
            
        })

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
