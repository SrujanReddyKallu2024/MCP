from stackadapt_shared.functions import get_env

env = get_env()

env_tag = {
        "test": "tst",
        "prod": "prd"
    }.get(env, env)  # Map environment for tag

tags = {

    "Account": f"eec-aws-uk-ms-consumersync-{env}",
    "Environment": env_tag,
    "AppID": "12962",
    "CostString": "2000.GB.328.402067",
    "ResourceOwner": "Targeting_Digital_L3_Resolver@experian.com"
}