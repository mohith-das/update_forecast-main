import os
import base64
import json
from flask import Flask, request, Response
from update_forecast import update_forecast

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    pubsub_message = envelope["message"]
    pubsub_message = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
    json_data = json.loads(pubsub_message)

    project_id = json_data['project_id']
    dataset_id = json_data['dataset_id']
    view_id = json_data['view_id']

    try:
        update_forecast(project_id, dataset_id, view_id)
        status_code = Response(status=200)
    except Exception as e:
        error_msg = f"project_id-{project_id} dataset_id-{dataset_id} view_id-{view_id} encountered error - {e}"
        print(error_msg)
        status_code = Response(status=204)
        # raise ValueError(error_msg)

    print(f"Function executed successfully : {project_id}:{dataset_id}:{view_id}")
    return status_code


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))