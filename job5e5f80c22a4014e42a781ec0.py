import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e5f80c22a4014e42a781ec1','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	pipe1_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e5f80c22a4014e42a781ec1", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e5f80c22a4014e42a781ec1','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e5f80c22a4014e42a781ec1','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e5f80c22a4014e42a781ec2','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	pipe1_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e5f80c22a4014e42a781ec1"],{"5e5f80c22a4014e42a781ec1": pipe1_DBFS}, "5e5f80c22a4014e42a781ec2", spark,json.dumps( {"FE": [{"feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "5024", "mean": "467.41", "stddev": "265.05", "min": "1", "max": "943", "missing": "0"}}, {"feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "5024", "mean": "435.2", "stddev": "336.41", "min": "1", "max": "1662", "missing": "0"}}, {"feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "5024", "mean": "3.51", "stddev": "1.13", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "5024", "mean": "3.52", "stddev": "0.46", "min": "1.51", "max": "4.8", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "5024", "mean": "16.15", "stddev": "9.11", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "5024", "mean": "6.86", "stddev": "4.35", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "5024", "mean": "1997.47", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e5f80c22a4014e42a781ec2','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e5f80c22a4014e42a781ec2','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e5f80c22a4014e42a781ec3','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	pipe1_AutoML = tpot_execution.Tpot_execution.run(["5e5f80c22a4014e42a781ec2"],{"5e5f80c22a4014e42a781ec2": pipe1_AutoFE}, "5e5f80c22a4014e42a781ec3", spark,json.dumps( {"model_type": "regression", "label": "AvgRating", "features": ["UserId", "MovieId", "Rating", "Timestamp", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "", "model_id": "5e5f8aca2a4014e42a781f47", "ProjectName": "demodeletetest", "PipelineName": "pipe1", "pipelineId": "5e5f80c22a4014e42a781ec0", "userid": "5e39734e0204cd465d4d2e10", "runid": "", "url_ResultView": "http://40.83.140.93:3200", "experiment_id": "551308251382540"}))

	PipelineNotification.PipelineNotification().completed_notification('5e5f80c22a4014e42a781ec3','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e5f80c22a4014e42a781ec3','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)

