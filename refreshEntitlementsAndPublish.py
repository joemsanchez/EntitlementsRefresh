import requests, json
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata
import uuid

##Establish a session with Tableau Server.
ts_username = ""
ts_password = ""
ts_url = "https://us-east-1.online.tableau.com/"
site = ""
hyper_file_path = ""

headers = {'accept': 'application/json','content-type': 'application/json'}
payload = { "credentials": {"personalAccessTokenName": ts_username, "personalAccessTokenSecret": ts_password, "site" :{"contentUrl": site} } }
print(payload)
req = requests.post(ts_url + 'api/3.5/auth/signin', json=payload, headers=headers, verify=True)
response =json.loads(req.content)
token = response["credentials"]["token"]
site_id = response["credentials"]["site"]["id"]
auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token}

#begin the file upload process
r = requests.post(ts_url + 'api/3.5/sites/'+site_id+'/fileUploads', headers=auth_headers)
upload_session_id = json.loads(r.content)['fileUpload']['uploadSessionId']

#Create a multipart upload.  For this sample, its only 1 chunk.  For better chunking info, see the sample
#scripts available at https://github.com/tableau/rest-api-samples/blob/master/python/publish_workbook.py
def _make_multipart(parts):
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type
with open(hyper_file_path, 'rb') as f:
    data = f.read()
    payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),'tableau_file': ('file', data, 'application/octet-stream')})
    server_response = requests.put(ts_url + 'api/3.5/sites/'+site_id+'/fileUploads/'+upload_session_id, data=payload, headers={'x-tableau-auth': token, "content-type": content_type})

    #find target datasource.  I've hard-coded the name here as "livetohyper" so just replace it with your desired target datasource
get_datasources = requests.get(ts_url + 'api/3.5/sites/'+site_id+'/datasources?filter=name:eq:SomeDataSource',headers = auth_headers)
datasources_response =json.loads(get_datasources.content)
ds_id = datasources_response['datasources']['datasource'][0]['id']


#put the data into your datasource.  simply enumerate which tables in which schemas you want to replace
request_id = str(uuid.uuid4())
auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token, 'RequestID':request_id}
print(request_id)
payload = {
    "actions": [
        {
            "action": "insert",
            "source-schema": "Extract",
            "source-table": "M_CHARTED",
            "target-schema": "Extract",
            "target-table": "M_CHARTED",
        },
        {
            "action": "insert",
            "source-schema": "Extract",
            "source-table": "M_CHARTED_DX",
            "target-schema": "Extract",
            "target-table": "M_CHARTED_DX",
        },
        {
            "action": "insert",
            "source-schema": "Extract",
            "source-table": "M_CHARTED_PROC",
            "target-schema": "Extract",
            "target-table": "M_CHARTED_PROC",
        }
    ]
}


patch_request = requests.patch(ts_url + 'api/3.19/sites/'+site_id+'/datasources/0093921b-d0ab-4de3-b5d7-16a899b98cab/data?uploadSessionId='+upload_session_id+'&append=true', headers = auth_headers, json = payload)
print(patch_request)
print(patch_request.content)
# Print status code
print("Status Code:", patch_request.status_code)

# Print the full response content
print("Response Content:", patch_request.content)

# If the response content is JSON, print it in a more readable format
try:
    print("JSON Response:", patch_request.json())
except ValueError:
    print("Response is not in JSON format")
