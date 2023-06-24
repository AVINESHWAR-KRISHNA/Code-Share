const fs = require('fs');
const path = require('path');
const requestp = require('request-promise');
const platformClient = require('purecloud-platform-client-v2');

const client = platformClient.ApiClient.instance;

// Configure the PureCloud environment and access token
client.setEnvironment(platformClient.PureCloudRegionHosts.us_west_2);
client.setAccessToken(client.authData.accessToken);

const exportContactList = function exportContactList(contactListId, outputPath) {
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
            fs.writeFile(outputPath, res, (err) => {
                if (err) {
                    console.error('Failed to write export contents:', err);
                } else {
                    console.log('Contents exported successfully');
                }
            });
        })
        .catch((err) => {
            console.error('Failed to export contact list:', err);
            if (err.body && err.body.code === 'no.available.list_export_uri') {
                console.log('Waiting for export...');
                setTimeout(() => exportContactList(contactListId, outputPath), 5000);
            }
        });
};

// Read command-line arguments or use default values
const clientId = process.argv[2] || '';
const clientSecret = process.argv[3] || '';
const contactListId = process.argv[4] || '';
const outputPath = process.argv[5] || '';

client.loginClientCredentialsGrant(clientId, clientSecret)
    .then(() => {
        const outboundApi = new platformClient.OutboundApi();
        return outboundApi.postOutboundContactlistExport(contactListId);
    })
    .then(() => {
        console.log('Contact list export initiated');
        exportContactList(contactListId, outputPath);
    })
    .catch((err) => {
        console.error('Failed to log in or initiate contact list export:', err);
    });
