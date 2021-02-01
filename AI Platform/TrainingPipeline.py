from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value


def create_training_pipeline_tabular_classification_sample(
    project: str,
    pipeline_name: str,
    dataset_id: str,
    model_display_name: str,
    target_column: str,
    transformations: list,
    predictionType: str,
    trainingFraction: int = 0.8,
    validationFraction: int = 0.1,
    testFraction: int = 0.1,

    location: str = "us-central1",
    api_endpoint: str = "us-central1-aiplatform.googleapis.com",

):
    """
    :param project str: Project ID
    :param pipeline_name str: Name of the Pipeline
    :param dataset_id str:
    :param model_display_name str:
    :param target_column str:
    :param transformations list: Columns to include with dtype, i.e. {'auto': {"column_name": "sepal_width"}}
    :param predictionType str: i.e. "classification" or "regression"
    :param trainingFraction int: Fraction of data used for training
    :param validationFraction int: Fraction of data used for in-training validation
    :param testFraction int: Fraction of data used for out of training validation/testing
    :param location str: Compute location
    :param api_endpoint str: Required for AI Platform API

    """

    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PipelineServiceClient(client_options=client_options)
    # transformations set the columns used for training and their data types

    training_task_inputs_dict = {
        # required inputs
        "targetColumn": target_column,
        "predictionType": predictionType,
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
        "display_name": pipeline_name,
        "training_task_definition": "gs://google-cloud-aiplatform/schema/trainingjob/definition/automl_tabular_1.0.0.yaml",
        "training_task_inputs": training_task_inputs,
        "input_data_config": {
            "dataset_id": dataset_id,
            "fraction_split": {
                "training_fraction": trainingFraction,
                "validation_fraction": validationFraction,
                "test_fraction": testFraction,
            },
        },
        "model_to_upload": {"display_name": model_display_name},
    }
    parent = f"projects/{project}/locations/{location}"
    response = client.create_training_pipeline(
        parent=parent, training_pipeline=training_pipeline
    )
    print("response:", response)