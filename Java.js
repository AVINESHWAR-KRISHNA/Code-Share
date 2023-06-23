const fs = require('fs');
const path = require('path');
const { createLogger, transports, format } = require('winston');

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
    new transports.File({ filename: path.join(logDir, 'script.log') })
  ]
});

// Log an error if the log directory or log file creation fails
logger.on('error', (error) => {
  console.error('Error occurred while creating the log file:', error);
});

const requestp = require('request-promise');
const platformClient = require('purecloud-platform-client-v2');
const client = platformClient.ApiClient.instance;

// Configure the PureCloud environment and access token
client.setEnvironment(platformClient.PureCloudRegionHosts.us-west-2);
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

// Read command-line arguments or use default values
const clientId = process.argv[2] || '667becd3-4eef-4346-bcb8-608afe2fac96';
const clientSecret = process.argv[3] || '2hieuwbroEmBcWysVd50-dyeb=2MDI-SAWILZRCISES';
const contactListId = process.argv[4] || 'Cab8d178210ad97a6156fca924';
const outputPath = process.argv[5] || '/path/to/file';

// Configure the logger to also print to the console
logger.add(new transports.Console());

// Login with client credentials and initiate contact list export
client.loginClientCredentialsGrant(clientId, clientSecret)
  .then(() => {
    logger.info('Logged in successfully');
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
