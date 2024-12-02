import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

@dlt.resource(write_disposition="replace")
def github_api_resource(api_secret_key: str = dlt.secrets.value):
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    for page in paginate(
        url,
        auth=BearerTokenAuth(api_secret_key), # type: ignore
        paginator=HeaderLinkPaginator(),
        params={"state": "open"}
    ):
        yield page

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='github_api_pipeline',
        destination='databricks',
        dataset_name='github_api_data',
        full_refresh=True,
    )

    # print credentials by running the resource
    data = list(github_api_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(github_api_resource())

    # pretty print the information on data that was loaded
    print(load_info)