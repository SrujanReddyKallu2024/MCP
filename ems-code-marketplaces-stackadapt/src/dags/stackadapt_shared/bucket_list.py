import os
import stackadapt_shared.functions 

buckets = {
    "source": "eec-aws-uk-ms-consumersync-{env}-source-bucket",
    "lookup": "eec-aws-uk-ms-consumersync-{env}-lookup-bucket",
    "idgraph": "eec-aws-uk-ms-consumersync-{env}-idgraph-bucket",
    "client_delivery": "eec-aws-uk-ms-consumersync-{env}-client-delivery-bucket",
    "consumersync_process": "eec-aws-uk-ms-consumersync-{env}-consumersync-process-bucket",
    "output": "eec-aws-uk-ms-consumersync-{env}-output-bucket",
    "interim": "eec-aws-uk-ms-consumersync-{env}-interim-bucket",
    "temp": "eec-aws-uk-ms-consumersync-{env}-temp-bucket",
    "stats": "eec-aws-uk-ms-consumersync-{env}-stats-bucket",
    "archive": "eec-aws-uk-ms-consumersync-{env}-archive-bucket",
    "code": "eec-aws-uk-ms-consumersync-{env}-code-bucket",
    "logs": "eec-aws-uk-ms-consumersync-{env}-logs-bucket"
}

def get_bucket_name(bucket_name, env=None):
    """
    Get the bucket name for the given internal shortcut bucket name and the environment.
    """
    if not env:
        env = stackadapt_shared.functions.get_env()
        
    return buckets[bucket_name].format(env=env)