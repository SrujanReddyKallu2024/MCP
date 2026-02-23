create me a pefrect readme.md  so first i iwll have to help the team with installation first where they can find is that is tell them that its comepletely regisrty specific https://mcp-registry.mn-na-sit.preprod-ascend-na.io/
vscdoe only allows any registry inside no one can modify or run theri local so make sure we have it in approved state and another one is
as soon you click install i will add images each leave space there i will push this into confluence so firt have full tbaler of contents assso n as click install you get an error dont worrry jsut make sure click on openconfigurrtaion and then you need tro make sure sure first u add       "args": [
		"--native-tls",
        "--index-url",
        "https://artifacts.experian.local/artifactory/api/pypi/pypi/simple",
        "ops-mcp-server@1.6.0",
      ],


nativce tls which wont be there by default adn then managed to fdfill in the the crdds 

{
  "servers": {
    "local.experian/ops-mcp-server-consumersync": {
      "type": "stdio",
      "command": "uvx",
      "args": [
		"--native-tls",
        "--index-url",
        "https://artifacts.experian.local/artifactory/api/pypi/pypi/simple",
        "ops-mcp-server@1.6.0",
      ],
      "env": {
        "AWS_REGION": "eu-west-2",
        "DEFAULT_ENV": "dev",
        "AWS_PROFILE_DEV": "consumersync",
        "AWS_PROFILE_UAT": "consumersync-uat",
        "AWS_PROFILE_TEST": "consumersync-test",
        "AWS_PROFILE_PROD": "consumersync-prod",
        "MWAA_ENV_DEV": "eec-aws-uk-ms-dev-consumersyncenv-mwaa",
        "MWAA_ENV_UAT": "eec-aws-uk-ms-uat-consumersync-mwaa",
        "MWAA_ENV_TEST": "eec-aws-uk-ms-tst-consumersync-mwaa",
        "MWAA_ENV_PROD": "eec-aws-uk-ms-prod-consumersync-mwaa",
        "EMR_LOG_BUCKET_DEV": "eec-aws-uk-ms-consumersync-dev-logs-bucket",
        "EMR_LOG_BUCKET_UAT": "eec-aws-uk-ms-consumersync-uat-logs-bucket",
        "EMR_LOG_BUCKET_TEST": "eec-aws-uk-ms-consumersync-tst-logs-bucket",
        "EMR_LOG_BUCKET_PROD": "eec-aws-uk-ms-consumersync-prod-logs-bucket",
        "EMR_LOG_PREFIX": "spark-logs",
        "CONFLUENCE_BASE_URL": "https://pages.experian.local",
        "CONFLUENCE_PAT": "MDM4NDkzNzU5NjQzOuUPeCDj2j1vrHbz7oGUvUYN6BEr",
        "CONFLUENCE_SPACE_KEY": "ACTIVATE",
        "AZDO_BASE_URL": "https://ukfhpapcvt02.uk.experian.local/tfs/DefaultCollection",
        "AZDO_PAT": "xoa44ayhkx2spwhab6pcxu5oug3w4rojfrq5ftzx7squl6fb5k6q",
        "AZDO_PROJECT": "Activate",
        "AZDO_TEAM": "Activate Team",
      },
      "gallery": "https://aimcpregistry.mn-na-sit.preprod-ascend-na.io",
      "version": "1.6.0",
    },
  },
  "inputs": [],
}




this should be final; with all correct place then so this is must and then click start add placehodler for image set tht etoken srespectively so carwefully give user instructiuon how to put it or get them accordingly confluenc ena daxure so onc ehe fills them the server is read and make sure you have uvcx isntalled so run the powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

[Environment]::SetEnvironmentVariable(
    "Path",
    $env:Path + ";$env:USERPROFILE\.local\bin",
    [EnvironmentVariableTarget]::User
)


for windows in powersheel to have it or restart if needed like keep it proper tell user it has tob e full profesisonal asnd then tell user with all the steops once he starts running so he soulou d be able to esee his tools thsere beside small icon vs code i will add limage and then he is good to go run and then clelasllery expain eaach tool end to end liek what the scorp and finally give simple way how to use


i want full podfessional way of doucment full  dioc.md so get it fully don e end to end full prefect dont missi any


also tell user to be on agent model 

tomorrow I need to deploy ID graphs to prod based on the confluence document can you help me understand what input files am I missing.
you can get the inut files list from confluence
what are all the files I would require to transfer
 
kodtha ninnu
 
ayya meere annaru ani chesa
 
first kocnham context iyyu daniki
 
na credits ayipoyanai
 
first eh confulenbc eget me dteials of that cobnfulence anu
 
then emaina adgu istadi
 


liek dont bump giove some context and start ahead