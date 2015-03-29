# Copyright 2015 Google Cloud Platform Book ISBN - 978-1-4842-1005-5_Krishnan_Ch01_The Basics of Cloud Computing
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import json

from httplib2 import Http

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build

DRIVE_API_SCOPE = 'https://www.googleapis.com/auth/drive.readonly'

#### Do not expose the following information -JSON_KEY_PATH, SERVICE_ACCOUNT_EMAIL, PRIVATE_KEY_PATH- in production environments. 
#### You can this in a configuration file.

# Use this variable when using a JSON key file downloaded from Google Developer Console
JSON_KEY_PATH = '<path-to-your-json-key-file.json>' 

# Use these variables when downloading the P12 key file directly from Google Developer Console
SERVICE_ACCOUNT_EMAIL = '<service-account-email>' # *@developer.gserviceaccount.com'
PRIVATE_KEY_PATH = '<path-to-your-pkcs-key.p12>' 

def createCredentials():

	client_email, private_key = loadPrivateKey()
	credentials = SignedJwtAssertionCredentials(client_email, private_key, DRIVE_API_SCOPE)

	return credentials


def loadPrivateKey():

	use_json_key = JSON_KEY_PATH is not None and JSON_KEY_PATH != '<path-to-your-json-key-file.json>'

	try:
 		key_text = open(JSON_KEY_PATH if use_json_key else PRIVATE_KEY_PATH, 'rb').read()
	except IOError as e:
		sys.exit('Error while reading private key: %s' % e)

	if use_json_key:
		json_key = json.loads(key_text)
		return json_key['client_email'], json_key['private_key']

	else:
		return SERVICE_ACCOUNT_EMAIL, key_text

	return client_email, private_key


def main():

	# 1. Generate authorization code or JWT
	credentials = createCredentials()

	# If you want to generate and show the current access token		
	# credentials._refresh(Http().request)
	# print credentials.access_token

	# 2. Authorize credentials
	http_auth = credentials.authorize(Http())

	# 3. Access API
	response, content = http_auth.request('https://www.googleapis.com/drive/v2/files?alt=json', 'GET')

	# 3. Access API using apiclient.discovery
	# drive_service = build('drive', 'v2', http_auth)
	# content = drive_service.files().list().execute()

	print json.dumps(content)

if __name__ == '__main__':
    main()