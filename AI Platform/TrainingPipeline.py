from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value


def create_training_pipeline_tabular_classification_sample(
    project: str,
    display_name: str,
    dataset_id: str,
    model_display_name: str,
    target_column: str,
    location: str = "us-central1",
    api_endpoint: str = "us-central1-aiplatform.googleapis.com",
):
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PipelineServiceClient(client_options=client_options)
    # set the columns used for training and their data types
    transformations = [
        {"auto": {"column_name": "sepal_width"}},
        {"auto": {"column_name": "sepal_length"}},
        {"auto": {"column_name": "petal_length"}},
        {"auto": {"column_name": "petal_width"}},
    ]

    training_task_inputs_dict = {
        # required inputs
        "targetColumn": target_column,
        "predictionType": "classification",
        "transformations": transformations,
        "trainBudgetMilliNodeHours": 8000,
        # optional inputs
        "disableEarlyStopping": False,
        # supported binary classification optimisation objectives:
        # maximize-au-roc, minimize-log-loss, maximize-au-prc,
        # maximize-precision-at-recall, maximize-recall-at-precision
        # supported multi-class classification optimisation objective:
        # minimize-log-loss
        "optimizationObjective": "minimize-log-loss",
        # possibly required inputs
        # required when using maximize-precision-at-recall
        # "optimizationObjectiveRecallValue": 0.5, # 0.0 - 1.0
        # required when using maximize-recall-at-precision
        # "optimizationObjectivePrecisionValue": 0.5, # 0.0 - 1.0
    }
    training_task_inputs = json_format.ParseDict(training_task_inputs_dict, Value())

    training_pipeline = {
        "display_name": display_name,
        "training_task_definition": "gs://google-cloud-aiplatform/schema/trainingjob/definition/automl_tabular_1.0.0.yaml",
        "training_task_inputs": training_task_inputs,
        "input_data_config": {
            "dataset_id": dataset_id,
            "fraction_split": {
                "training_fraction": 0.8,
                "validation_fraction": 0.1,
                "test_fraction": 0.1,
            },
        },
        "model_to_upload": {"display_name": model_display_name},
    }
    parent = f"projects/{project}/locations/{location}"
    response = client.create_training_pipeline(
        parent=parent, training_pipeline=training_pipeline
    )
    print("response:", response)