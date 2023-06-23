const fs = require('fs');
const path = require('path');
const { createLogger, transports, format } = require('winston');
const requestp = require('request-promise');
const platformClient = require('purecloud-platform-client-v2');

// Create the log directory
const logDir = 'jslogs';
fs.mkdirSync(logDir, { recursive: true });

// Create a logger with Winston
const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.printf((info) => `${info.timestamp} ${info.level}: ${info.message}`)
  ),
  transports: [
    new transports.File({ filename: path.join(logDir, 'script.log') }),
    new transports.Console()
  ]
});

// Log an error if the log directory or log file creation fails
logger.on('error', (error) => {
  console.error('Error occurred while creating the log file:', error);
});

// Configure the PureCloud environment and access token
const client = platformClient.ApiClient.instance;
client.setEnvironment(platformClient.PureCloudRegionHosts.us-west-2);

// Export the contact list
const exportContactList = async (contactListId, outputPath) => {
  try {
    logger.info('Exporting contact list...');
    const outboundApi = new platformClient.OutboundApi();
    const res = await outboundApi.getOutboundContactlistExport(contactListId, { download: 'false' });
    const downloadUri = res.uri;
    const data = await requestp({
      uri: downloadUri,
      headers: {
        'Authorization': `bearer ${client.authData.accessToken}`
      }
    });
    await fs.promises.writeFile(outputPath, data);
    logger.info('Contents exported successfully');
  } catch (err) {
    logger.error('Failed to export contact list:', err);
    if (err.body && err.body.code === 'no.available.list_export_uri') {
      logger.info('Waiting for export...');
      setTimeout(() => exportContactList(contactListId, outputPath), 5000);
    }
  }
};

// Read command-line arguments or use default values
const [clientId, clientSecret, contactListId, outputPath] = process.argv.slice(2);

// Run the script
const runScript = async () => {
  try {
    // Configure the logger to also print to the console
    logger.add(new transports.Console());

    // Login with client credentials and initiate contact list export
    client.setAccessToken(await client.loginClientCredentialsGrant(clientId, clientSecret));
    const outboundApi = new platformClient.OutboundApi();
    await outboundApi.postOutboundContactlistExport(contactListId);

    await exportContactList(contactListId, outputPath);
    logger.info('Script execution completed.');
  } catch (err) {
    logger.error('Failed to log in or initiate contact list export:', err);
  }
};

runScript();
