const fs = require('fs');
const path = require('path');
const requestp = require('request-promise');
const platformClient = require('purecloud-platform-client-v2');
const { createLogger, transports, format } = require('winston');
const { Script } = require('vm');

// Retrieve the log file path
const logDir = 'JS_LOGS';
const logFile = path.join(logDir, 'Script.log');

const logger = createLogger({
    level: 'info',
    format: format.combine(
        format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        format.printf((info) => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
        new transports.File({ filename: logFile }),
        new transports.Console()
    ]
});

const client = platformClient.ApiClient.instance;

client.setEnvironment(platformClient.PureCloudRegionHosts.us_west_2);
client.setAccessToken(client.authData.accessToken);

const exportContactList = function exportContactList(contactListId, outputPath) {
    logger.info('Exporting contact list...');

    const outboundApi = new platformClient.OutboundApi();
    outboundApi.getOutboundContactlistExport(contactListId, { download: 'false' })
        .then((res) => {
            const downloadUri = res.uri;
            return requestp({
                uri: downloadUri,
                headers: {
                    'Authorization': `bearer ${client.authData.accessToken}`
                }
            });
        })
        .then((res) => {
            logger.info('Export contents retrieved');

            fs.writeFile(outputPath, res, (err) => {
                if (err) {
                    logger.error('Failed to write export contents:', err);
                } else {
                    logger.info('Contents exported successfully');
                    logger.info('Process Completed Successfully.\n');
                    logger.info('------------------------------------------------------------------------------------------------------------');
                }
            });
        })
        .catch((err) => {
            logger.error('Failed to export contact list:', err);
            if (err.body && err.body.code === 'no.available.list_export_uri') {
                logger.info('Waiting for export...');
                setTimeout(() => exportContactList(contactListId, outputPath), 5000);
            }
        });
};

const clientId = process.argv[2] || '';
const clientSecret = process.argv[3] || '';
const contactListId = process.argv[4] || '';
const outputPath = process.argv[5] || '';

logger.info('Logging Started...');
logger.info(contactListId);

client.loginClientCredentialsGrant(clientId, clientSecret)
    .then(() => {
        logger.info('Logged in successfully with Client ID and Client Secret.');
        const outboundApi = new platformClient.OutboundApi();
        return outboundApi.postOutboundContactlistExport(contactListId);
    })
    .then(() => {
        logger.info('Contact list export initiated');
        exportContactList(contactListId, outputPath);
    })
    .catch((err) => {
        logger.error('Failed to log in or initiate contact list export:', err);
    });



import os
import requests
import platform
import time
from datetime import datetime
from requests.exceptions import RequestException
from logging import getLogger, StreamHandler, FileHandler, Formatter, INFO

# Set up logger
log_dir = 'PY_LOGS'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'Script.log')

logger = getLogger(__name__)
logger.setLevel(INFO)

formatter = Formatter('%(asctime)s %(levelname)s: %(message)s')
console_handler = StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Set up PureCloud API client
client_id = input("Enter Client ID: ")
client_secret = input("Enter Client Secret: ")
contact_list_id = input("Enter Contact List ID: ")
output_path = input("Enter Output Path: ")

auth_url = 'https://login.mypurecloud.com/oauth/token'
api_url = 'https://api.mypurecloud.com'

headers = {
    'Content-Type': 'application/json'
}

data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}

try:
    response = requests.post(auth_url, headers=headers, json=data)
    response.raise_for_status()
    auth_data = response.json()

    access_token = auth_data['access_token']

    headers['Authorization'] = f"Bearer {access_token}"

    logger.info('Logged in successfully with Client ID and Client Secret.')

    # Initiate contact list export
    export_url = f"{api_url}/api/v2/outbound/contactlists/{contact_list_id}/export"
    response = requests.post(export_url, headers=headers)
    response.raise_for_status()

    logger.info('Contact list export initiated.')

    # Retrieve export contents
    export_id = response.json()['id']
    retrieve_url = f"{api_url}/api/v2/outbound/contactlists/{contact_list_id}/exports/{export_id}"
    completed = False

    while not completed:
        time.sleep(5)
        response = requests.get(retrieve_url, headers=headers)
        response.raise_for_status()
        export_status = response.json()['status']

        if export_status == 'completed':
            completed = True
            download_uri = response.json()['downloadUri']

            # Download export contents
            response = requests.get(download_uri, headers=headers)
            response.raise_for_status()

            logger.info('Export contents retrieved.')

            with open(output_path, 'wb') as file:
                file.write(response.content)

            logger.info('Contents exported successfully.')
            logger.info('Process Completed Successfully.')
            logger.info('------------------------------------------------------------------------------------------------------------')

        elif export_status == 'failed':
            logger.error('Failed to export contact list.')
            completed = True

except RequestException as e:
    logger.error('Request Exception:', e)

except Exception as e:
    logger.error('Exception:', e)
