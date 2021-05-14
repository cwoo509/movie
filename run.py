from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request
from flask_cors import CORS
from dbmodule import dbModule

oracle_db = dbModule.Database()

app = Flask(__name__)
cors = CORS(app)

# scheduler = BackgroundScheduler(daemon=True)
# scheduler.start()
# scheduler.add_job(db_save, 'cron', hour=0)

@app.route('/', methods=['GET'])
def example():
    result = 0
    return result

if __name__ == "__main__":
    app.run(port="8082")