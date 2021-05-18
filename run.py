from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request
from flask_cors import CORS
from dbmodule import dbModule

from contentsbased import get_contentbased_recommendation as cb
from userbased import get_userbased_recommend as ub
# from fast_userbased import get_userbased_recommend as ub
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

@app.route('/main', methods=['GET'])
def main():
    M_ID = request.args.get('M_ID')
    cbrec = cb(M_ID)
    ubrec = ub(M_ID)
    return jsonify(cbrec, ubrec)
if __name__ == "__main__":
    app.run(port="8082")