from .context import Context

class AwsClient:
    def __init__(self, ctx : Context, api_name : str, region=None):
        self.ctx = ctx
        self.client = ctx.client(api_name, region)

    @staticmethod
    def response_kv_to_dict(response, token='Tags', key='Key', value='Value'):
        if token in response:
            tags = { x[key]:x[value] for x in response[token] }
        else:
            tags = {}
        return tags  

    def check_response(self, resp):
        if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
            return None
        raise ValueError({'message':"Boto3 error", 'response':resp})
