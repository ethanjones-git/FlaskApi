'''
MAIN API
'''
import pandas as pd
import os
import sys
print(sys.version)
from api_polypinion import articles_and_ranks, categorical_pull
from flask_cors import CORS
from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse
from intr_sql import commit_interactions


path = os.getcwd() +'/data/'
art_path,rank_path = path+'articles.csv', path+'rank.csv'

app = Flask(__name__)
api = Api(app)
CORS(app)

# parsing

task_post_args = reqparse.RequestParser()
task_post_args.add_argument("key_id")
task_post_args.add_argument("sessionId")
task_post_args.add_argument("eventType")
task_post_args.add_argument("timestamp")


class all_(Resource):
    def get(self):
        out = articles_and_ranks()
        return out

class politics(Resource):
    def get(self):
        out = categorical_pull(['Politics'])
        return out

class worldnews(Resource):
    def get(self):
        out = categorical_pull(['World News'])
        return out

class business(Resource):
    def get(self):
        out = categorical_pull(['Business'])
        return out

class finance(Resource):
    def get(self):
        out = categorical_pull(['Finance'])
        return out

class sports(Resource):
    def get(self):
        out = categorical_pull(['Sports'])
        return out

class arts(Resource):
    def get(self):
        out = categorical_pull(['Arts & Entertainment'])
        return out

class science(Resource):
    def get(self):
        out = categorical_pull(['Science'])
        return out

class environment(Resource):
    def get(self):
        out = categorical_pull(['Environment'])
        return out

class environment(Resource):
    def get(self):
        out = categorical_pull(['Misc'])
        return out

class interactions(Resource):
    def post(self):
        out = task_post_args.parse_args()
        commit_interactions(key_id=out['key_id'],sessionId=out['sessionId'],eventType=out['eventType'],timestamp = out['timestamp'])
        print(out)
        return 200

api.add_resource(all_,"/all")
api.add_resource(politics,"/politics")
api.add_resource(worldnews,"/worldnews")
api.add_resource(business,"/business")
api.add_resource(finance,"/finance")
api.add_resource(sports,"/sports")
api.add_resource(arts,"/arts")
api.add_resource(science,"/science")
api.add_resource(environment,"/environment")
#api.add_resource(dyn,"/<string:cat>")

#push
api.add_resource(interactions,"/interactions")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)


