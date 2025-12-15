import boto3
from botocore.exceptions import ClientError
from .schemas import ConnectionInfo

def get_db_connection_info() -> ConnectionInfo:
    """
    Retrieve AWS Secrets Manager secret for seafloor geodesy RDS database.

    This function retrieves database credentials and connection information stored 
    in AWS Secrets Manager for the seafloor_geodesy_rds secret in the us-east-2 region.

    Returns:
        str: A JSON string containing the secret value with database credentials 
             and connection parameters.

    Raises:
        ClientError: If there is an error retrieving the secret from AWS Secrets Manager.
                     This includes cases where the secret doesn't exist, permissions are
                     insufficient, or network issues occur. See AWS Secrets Manager API
                     documentation for complete list of possible exceptions:
                     https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html

    Note:
        This function uses hardcoded values for secret_name and region_name. 
        AWS credentials must be configured in the environment (e.g., via AWS CLI,
        environment variables, or IAM role).
    """

    secret_name = "seafloor_geodesy_rds"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    try:
        connection_info = ConnectionInfo.model_validate_json(secret)
    except Exception as e:
        raise ValueError("Failed to parse secret JSON into ConnectionInfo model.") from e
    return connection_info