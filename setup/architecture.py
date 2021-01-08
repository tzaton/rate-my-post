from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import EMRCluster, Glue, GlueDataCatalog
from diagrams.aws.compute import ECS, Lambda
from diagrams.aws.database import Dynamodb
from diagrams.aws.ml import Sagemaker
from diagrams.aws.network import APIGateway
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Spark
from diagrams.onprem.client import User
from diagrams.onprem.compute import Server
from diagrams.onprem.network import Internet

with Diagram(name="", show=False, direction="LR",
             filename="setup/architecture",
             graph_attr={"dpi": "300"}) as diag:
    with Cluster("Source"):
        source = Server("HTTP")
    with Cluster("Data load"):
        storage = S3("Data storage")
        download = ECS("ECS download task")
        unzip_trigger = Lambda("S3 event trigger")
        unzip = ECS("ECS unzip task")
    with Cluster("Data processing"):
        parse = Glue("Glue Spark XML\nparser")
        catalog = GlueDataCatalog("Data Catalog")
        with Cluster("Feature engineering"):
            train_features = Glue("Glue Spark job:\ntrain features")
            predict_features = Glue("Glue Spark job:\nprediction features")
        prediction_db = Dynamodb("Prediction database")
    with Cluster("ML model"):
        cluster = EMRCluster("EMR Cluster")
        model_fit = Spark("Spark + MLeap")
        model = Sagemaker("Sagemaker endpoint")
    with Cluster("Serving"):
        app = Internet("Web app")
        api = APIGateway("REST API")
        predict_trigger = Lambda("Request/response\nhandler")
    user = User("End user")

    source >> download >> storage >> unzip_trigger >> unzip >> storage
    storage >> parse >> catalog
    catalog >> [train_features, predict_features]
    predict_features >> prediction_db >> predict_trigger
    train_features >> cluster >> model_fit >> model
    predict_trigger >> model >> predict_trigger
    storage >> app
    user >> Edge() << app >> api >> predict_trigger >> api >> app
