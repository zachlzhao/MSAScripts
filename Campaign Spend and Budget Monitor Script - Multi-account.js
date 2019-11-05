const EMAIL_RECIPIENTS = [''];

// See this page to get Google credentials: https://docs.microsoft.com/en-us/advertising/scripts/examples/authenticating-with-google-services
// If you chose option 1 in Getting an access token, set accessToken to 
// the token you received from Google OAuth playground. Otherwise, if you
// chose option 2, set clientId, clientSecret, and refreshToken.

const credentials = {
    accessToken: '',
    clientId: '',
    clientSecret: '',
    refreshToken: ''
};

//Set a notification threshold so that only campaigns whose spend pacing exceeds the threshold will be included in the report.
//For example, if you would like to only include campaigns whose spend today exceeds 90% of the daily budget, set the following as 90.
//If you would like to include all spending campaigns regardless of the pacing threshold, set the following to 0.

const BUDGET_PACING_NOTIFICATION_THRESHOLD = 0;

//Use the two variables below to set the format and timezone of the date time, which will appear in the email notification and name of the spreadsheet.
//Use DATETIME_LOCALE to set the format. A list of locales can be found here https://www.w3schools.com/jsref/jsref_tolocalestring.asp
//Use DATETIME_TIMEZONE to set the timezone. A list of timezones can be found here https://docs.microsoft.com/en-us/advertising/scripts/concepts/timezone-mapping

const DATETIME_LOCALE = 'en-US';
const DATETIME_TIMEZONE = 'America/New_York';

//Set the following to true if you want an email to be generated even if no campaign is found that meets the criterion. 

const SEND_EMAIL_EVEN_IF_NO_ROWS_FOUND = false;

//Set the following to true if you would like all accounts to be combined into a single tab, instead of one account per tab. 

const COMBINE_ALL_ACCOUNTS_IN_ONE_SHEET = true;

//Set the following to true if you want a new sheet to be created every time the script runs; 
//set it to false if you would like the same sheet to be OVERWRITTEN every time the script runs.

const CREATE_NEW_SPREADSHEET_ON_EACH_RUN = true;

const REPORT_NAME = 'Campaigns and budgets';
const ACCOUNTS_FILE_NAME = `MicrosoftAds-Scripts-${REPORT_NAME}-AccountList.json`;
const SPREADSHEET_FILE_NAME = `MicrosoftAds-Scripts-${REPORT_NAME}`;
const ACCOUNT_BATCH_SIZE = 50;

function* getEntities() {
    const campaigns = AdsApp.campaigns()
        .withCondition("Clicks >= 1")
        .forDateRange("TODAY")
        .get();

    while (campaigns.hasNext()) {
        yield campaigns.next();
    }
}

function* getAccounts() {
    const accounts = AccountsApp.accounts()
        .withCondition("Impressions >=1")
        .forDateRange("LAST_7_DAYS")
        .get();

    while (accounts.hasNext()) {
        yield accounts.next();
    }
}

const columns = [
    { name: 'Account Name', func: _ => currentAccount.getName() },
    { name: 'Account Number', func: _ => currentAccount.getAccountNumber() },
    { name: 'Campaign Name', func: camp => camp.getName() },
    { name: 'Campaign Id', func: camp => camp.getId() },
    { name: 'Budget Amount', func: camp => camp.getBudget().getAmount() },
    { name: 'Spend Today', func: camp => camp.getStats().getCost() },
    { name: 'Budget Pacing', func: camp => Math.floor(camp.getStats().getCost()/camp.getBudget().getAmount()*100) + "%"},
    { name: 'Impressions', func: camp => camp.getStats().getImpressions() },
    { name: 'Clicks', func: camp => camp.getStats().getClicks() }
];


let currentAccount;

function main() {
    const remainingQuota = UrlFetchApp.getRemainingDailyQuota();

    if (remainingQuota < 50) {
        Logger.log(`Remaining URL fetch quota ${remainingQuota} is not enough to run this script now`);

        return;
    }

    // If it's the first time the script has run, the function creates
    // the JSON file. The file contains a list of JSON objects.
    // Each object contains an account ID field (id) and a time
    // stamp field (lastChecked) when the account was last processed

    createFileIfNotExists(ACCOUNTS_FILE_NAME, false);

    // Load the list of account objects from the JSON file or if the file is empty,
    // set the list to empty.

    const accountsList = loadObject(ACCOUNTS_FILE_NAME) || [];

    const accounts = getAccounts();

    const filteredAccountIds = [];

    //@ts-ignore
    for (const account of accounts) {
        const accountId = account.getAccountId();

        filteredAccountIds.push(accountId);

        // Add new accounts to the saved account list.

        if (!accountsList.some(x => x.id == accountId)) {
            accountsList.push({
                id: account.getAccountId(),
                lastChecked: ''
            });
        }
    }

    // Remove accounts from the saved account list if they didn't pass the filter condition.

    const filteredAccountsList = accountsList.filter(x => filteredAccountIds.indexOf(x.id) != -1);

    // Sort the list by lastChecked. This ensures that unprocessed accounts 
    // (i.e., lastChecked: '') in accountsList are first in the list. If all 
    // accounts have been processed, the accounts with the oldest lastChecked 
    // time stamp are first.

    filteredAccountsList.sort((a, b) => a.lastChecked.localeCompare(b.lastChecked));

    // Save the list of JSON objects back to the file. The file is opened
    // again in reportResults() to update the lastChecked field with
    // the time stamp of when the account was processed.

    saveObject(filteredAccountsList, ACCOUNTS_FILE_NAME);

    // Get a maximum of the first 50 accounts (ACCOUNT_BATCH_SIZE) from accountsList.
    // Each time the script runs, it grabs the next 50 accounts until eventually
    // The script processes all the accounts.

    const accountsToCheck = filteredAccountsList.slice(0, ACCOUNT_BATCH_SIZE);

    const accountIdsToCheck = accountsToCheck.map(x => x.id);

    Logger.log('Will process the following accounts: ' + JSON.stringify(accountIdsToCheck));

    // Use the executeInParallel method to execute the findEntities function
    // for up to 50 accounts in parallel. When findEntities finishes for all
    // accounts, the reportResults function will be called. 

    AccountsApp.accounts().withIds(accountIdsToCheck).executeInParallel('findEntities', 'reportResults', JSON.stringify(accountsToCheck));
}

function findEntities(accountsToCheckJson) {
    currentAccount = AdsApp.currentAccount();

    const currentAccountInfo = `${currentAccount.getName()} ${currentAccount.getAccountId()} (${currentAccount.getAccountNumber()})`;

    Logger.log(`Processing account: ${currentAccountInfo}`);

    const reportRows = [];

    const entities = getEntities();

    //@ts-ignore
    for (const entity of entities) {
        const entityRow = columns.reduce((temp, column) => { temp[column.name] = column.func(entity); return temp }, {});
        // Add an if statement to further limit the entities being checked.
        //(in this case, only check campaigns whose pacing is above the notification threshold.)

        if (Math.floor(entity.getStats().getCost()/entity.getBudget().getAmount()*100) > BUDGET_PACING_NOTIFICATION_THRESHOLD) {
        reportRows.push(entityRow);
        }
    }

    Logger.log(`Found ${reportRows.length} rows for account ${currentAccountInfo}`);

    return JSON.stringify({
        customerId: currentAccount.getCustomerId(),
        accountId: currentAccount.getAccountId(),
        accountNumber: currentAccount.getAccountNumber(),
        accountName: currentAccount.getName(),
        rowCount: reportRows.length,
        reportData: { rows: reportRows }
    });
}

const dateTimeStr = new Date().toLocaleString(DATETIME_LOCALE, { timeZone: DATETIME_TIMEZONE }).replace(/\u200e/g, '');

function reportResults(results) {
    Logger.log('Reporting results');

    const sheetResults = [];

    const accountResults = [];

    const accountsList = loadObject(ACCOUNTS_FILE_NAME);

    const accountsById = accountsList.reduce((map, acc) => (map[acc.id] = acc, map), {});

    for (const result of results) {
        if (!result.getReturnValue()) {
            Logger.log(`Got an error in result: ${result.getError()}`);

            continue;
        }

        const accountResult = JSON.parse(result.getReturnValue());

        accountResults.push(accountResult);

        accountsById[accountResult.accountId].lastChecked = dateTimeStr;
    }

    if (COMBINE_ALL_ACCOUNTS_IN_ONE_SHEET) {
        const combinedRows = [];

        for (const accountResult of accountResults) {
            combinedRows.push(...accountResult.reportData.rows);
        }

        sheetResults.push({ sheetData: { reportData: { rows: combinedRows }, rowCount: combinedRows.length }, sheetName: 'All accounts' });
    } else {
        for (const accountResult of accountResults) {
            const sheetName = `${accountResult.accountId} (${accountResult.accountNumber})`;

            sheetResults.push({ sheetData: accountResult, sheetName: sheetName });
        }
    }

    const spreadsheetName = CREATE_NEW_SPREADSHEET_ON_EACH_RUN
        ? `${SPREADSHEET_FILE_NAME} ${dateTimeStr}`
        : SPREADSHEET_FILE_NAME;

    const spreadsheetId = createFileIfNotExists(spreadsheetName, true);

    const sheets = getOrCreateSheets(spreadsheetId, sheetResults.map(x => x.sheetName));

    const sheetsByName = sheets.reduce((map, sheet) => (map[sheet.properties.title] = sheet, map), {});

    const spreadsheetRows = [];

    const summaryEmailData = [];

    const headers = columns.map(x => x.name);

    for (const sheetResult of sheetResults) {
        const sheetData = sheetResult.sheetData;

        const sheetRows = [headers];

        for (const row of sheetData.reportData.rows) {
            const rowValues = headers.map(x => row[x]);

            sheetRows.push(rowValues);
        }

        const sheetId = sheetsByName[sheetResult.sheetName].properties.sheetId;

        spreadsheetRows.push({ sheetId: sheetId, rows: sheetRows });

        summaryEmailData.push({
            customerId: sheetData.customerId,
            accountId: sheetData.accountId,
            accountNumber: sheetData.accountNumber,
            accountName: sheetData.accountName,
            rowCount: sheetData.rowCount,
            sheetId: sheetId
        });
    }

    writeRowsToSpreadsheet(spreadsheetRows, spreadsheetId);

    sendSummaryEmailIfNeeded(summaryEmailData, spreadsheetId);

    saveObject(accountsList, ACCOUNTS_FILE_NAME);
}

function sendSummaryEmailIfNeeded(summaryEmailData, spreadsheetId) {
    if (summaryEmailData.length == 0) {
        return;
    }

    const totalrowCount = summaryEmailData.reduce((sum, accountData) => sum + accountData.rowCount, 0);

    if (totalrowCount == 0 && !SEND_EMAIL_EVEN_IF_NO_ROWS_FOUND) {
        Logger.log("Skipping email notification since no rows were found");

        return;
    }

    const subject = `MSA: Found ${totalrowCount} campaigns(s) whose spend exceeds ${BUDGET_PACING_NOTIFICATION_THRESHOLD}% of budget`;

    const fileUrl = shareFileWithLink(spreadsheetId);

    const messageHtml =
        `<html>
    <body>
    ${subject}
    <br/ ><br/ >
    <table border="1" width="95%" style="border-collapse:collapse;">
        <tr>
            ${COMBINE_ALL_ACCOUNTS_IN_ONE_SHEET ? '' : `
            <th align="left">Customer Id</th>
            <th align="left">Account Id</th>
            <th align="left">Account Number</th>
            <th align="left">Account Name</th>
            `}
            <th align="center"># of Campaigns</th>
            <th align="center">Full Report</th>
        </tr>
        ${summaryEmailData.map(row =>
            `<tr>
            ${COMBINE_ALL_ACCOUNTS_IN_ONE_SHEET ? '' : `
            <td align="left">${row.customerId}</td>
            <td align="left">${row.accountId}</td>
            <td align="left">${row.accountNumber}</td>
            <td align="left">${row.accountName}</td>
            `}
            <td align="center">${row.rowCount}</td>
            <td align="center"><a href="${fileUrl}#gid=${row.sheetId}">Show Details</a></td>
        </tr>`).join('')}
    </table>
    <br/ >
    Checked: ${dateTimeStr}
    </body>
</html>`;

    for (const address of EMAIL_RECIPIENTS) {
        const email = `To: ${address}
Subject: ${subject}
Content-Type: text/html; charset="utf-8"

${messageHtml}`;

        getGmailApi().users.messages.send({ userId: 'me' }, { raw: Base64.encode(email) });
    }
}

const getSheetsApi = (() => {
    let sheetsApi;
    return () => sheetsApi || (sheetsApi = GoogleApis.createSheetsService(credentials));
})();

const getDriveApi = (() => {
    let driveApi;
    return () => driveApi || (driveApi = GoogleApis.createDriveService(credentials));
})();

const getGmailApi = (() => {
    let gmailApi;
    return () => gmailApi || (gmailApi = GoogleApis.createGmailService(credentials));
})();

function getOrCreateSheets(spreadsheetId, sheetNames) {
    const spreadsheetResponse = getSheetsApi().spreadsheets.get({ spreadsheetId: spreadsheetId });

    const existingSheetsByName = spreadsheetResponse.result.sheets.reduce((map, sheet) => (map[sheet.properties.title] = sheet, map), {});

    const sheetNamesToCreate = sheetNames.filter(x => !existingSheetsByName[x]);

    if (sheetNamesToCreate.length == 0) {
        return spreadsheetResponse.result.sheets;
    }

    const newSheetsResponse = createSheets(spreadsheetId, sheetNamesToCreate);

    return spreadsheetResponse.result.sheets.concat(newSheetsResponse.updatedSpreadsheet.sheets);
}

function createSheets(spreadsheetId, sheetNames) {
    const requests = sheetNames.map(x => ({ addSheet: { properties: { title: x } } }));

    const response = getSheetsApi().spreadsheets.batchUpdate({ spreadsheetId: spreadsheetId }, {
        requests: requests,
        includeSpreadsheetInResponse: true
    }).result;

    return response;
}

function writeRowsToSpreadsheet(spreadsheetRows, spreadsheetId) {
    const requests = [].concat(...spreadsheetRows.map(sheetRows => [
        {
            updateSheetProperties: {
                properties: {
                    sheetId: sheetRows.sheetId,
                    gridProperties: {
                        rowCount: Math.max(sheetRows.rows.length, 1000)
                    }
                },
                fields: 'gridProperties.rowCount'
            }
        },
        {
            updateCells: {
                range: {
                    sheetId: sheetRows.sheetId
                },
                rows: sheetRows.rows.map(row => ({
                    values: row.map(columnValue => ({
                        userEnteredValue: { stringValue: columnValue ? columnValue.toString() : columnValue }
                    }))
                })),
                fields: '*'
            }
        }
    ]));

    getSheetsApi().spreadsheets.batchUpdate({ spreadsheetId: spreadsheetId }, {
        requests: requests,
        includeSpreadsheetInResponse: false
    });
}

function shareFileWithLink(fileId) {
    getDriveApi().permissions.create({ fileId: fileId }, {
        type: 'anyone',
        role: 'reader',
        allowFileDiscovery: false
    });

    const fileResponse = getDriveApi().files.get({ fileId: fileId, fields: 'webViewLink' }).result;

    return fileResponse.webViewLink;
}

function findFileId(fileName) {
    const req = escape(`name = '${fileName}'`);

    const searchResult = getDriveApi().files.list({ q: req }).result;

    if (searchResult.files.length > 0) {
        return searchResult.files[0].id;
    }

    return null;
}

function createFileIfNotExists(fileName, isSpreadsheet) {
    const existingFileId = findFileId(fileName);

    if (existingFileId) {
        return existingFileId;
    }

    const createResult = getDriveApi().files.create({}, {
        name: fileName,
        mimeType: isSpreadsheet ? 'application/vnd.google-apps.spreadsheet' : 'application/vnd.google-apps.document'
    }).result;

    return createResult.id;
}

function saveObject(obj, fileName) {
    const fileId = createFileIfNotExists(fileName, false);

    getDriveApi().files.update({ fileId: fileId }, JSON.stringify(obj), { uploadType: 'simple', contentType: 'text/plain' });
}

function loadObject(fileName) {
    const fileId = findFileId(fileName);

    if (!fileId) {
        throw new Error(`File ${fileName} not found`);
    }

    const fileData = getDriveApi().files.export({ fileId: fileId, mimeType: 'text/plain' }).body.trim();

    if (fileData) {
        return JSON.parse(fileData.trim());
    } else {
        return null;
    }
}

// Common Google library code that all Scripts that access Google
// services will include.

var GoogleApis;
(function (GoogleApis) {
    GoogleApis.createSheetsService = credentials => createService("https://sheets.googleapis.com/$discovery/rest?version=v4", credentials);
    GoogleApis.createDriveService = credentials => createService("https://www.googleapis.com/discovery/v1/apis/drive/v3/rest", credentials);
    GoogleApis.createGmailService = credentials => createService("https://www.googleapis.com/discovery/v1/apis/gmail/v1/rest", credentials);

    // Creation logic based on https://developers.google.com/discovery/v1/using#usage-simple
    function createService(url, credentials) {
        const content = UrlFetchApp.fetch(url).getContentText();
        const discovery = JSON.parse(content);
        const accessToken = getAccessToken(credentials);
        const standardParameters = discovery.parameters;
        const service = build(discovery, {}, discovery['rootUrl'], discovery['servicePath'], standardParameters, accessToken);
        return service;
    }

    function createNewMethod(method, rootUrl, servicePath, standardParameters, accessToken) {
        return (urlParams, body, uploadParams) => {
            let urlPath = method.path;
            if (uploadParams) {
                if (!method.supportsMediaUpload) {
                    throw new Error(`Media upload is not supported`);
                }
                const uploadProtocols = method.mediaUpload.protocols;
                const uploadType = uploadParams.uploadType;
                switch (uploadType) {
                    case 'simple':
                        const simpleProtocol = uploadProtocols.simple;
                        if (!simpleProtocol) {
                            throw new Error(`Upload protocol ${uploadType} is not supported`);
                        }
                        urlPath = simpleProtocol.path;
                        break;
                    case 'resumable':
                        const resumableProtocol = uploadProtocols.resumable;
                        if (!resumableProtocol) {
                            throw new Error(`Upload protocol ${uploadType} is not supported`);
                        }
                        urlPath = resumableProtocol.path;
                        break;
                    default:
                        throw new Error(`Unknown upload type ${uploadType}`);
                }
            }
            const queryArguments = [];
            for (const name in urlParams) {
                const paramConfg = method.parameters[name] || standardParameters[name];
                if (!paramConfg) {
                    throw new Error(`Unexpected url parameter ${name}`);
                }
                switch (paramConfg.location) {
                    case 'path':
                        urlPath = urlPath.replace('{' + name + '}', urlParams[name]);
                        break;
                    case 'query':
                        queryArguments.push(`${name}=${urlParams[name]}`);
                        break;
                    default:
                        throw new Error(`Unknown location ${paramConfg.location} for url parameter ${name}`);
                }
            }
            if (uploadParams) {
                queryArguments.push(`uploadType=${uploadParams.uploadType === 'simple' ? 'media' : uploadParams.uploadType}`);
            }
            let url = rootUrl;
            if (urlPath.startsWith('/')) {
                url += urlPath.substring(1);
            } else {
                url += servicePath + urlPath;
            }
            if (queryArguments.length > 0) {
                url += '?' + queryArguments.join('&');
            }
            const payload = uploadParams ? body : JSON.stringify(body);
            const contentType = uploadParams ? uploadParams.contentType : 'application/json';
            const fetchParams = { contentType: contentType, method: method.httpMethod, payload: payload, headers: { Authorization: `Bearer ${accessToken}` }, muteHttpExceptions: true };
            const httpResponse = UrlFetchApp.fetch(url, fetchParams);
            const responseContent = httpResponse.getContentText();
            const responseCode = httpResponse.getResponseCode();
            let parsedResult;
            try {
                parsedResult = JSON.parse(responseContent);
            } catch (e) {
                parsedResult = false;
            }
            const response = new Response(parsedResult, responseContent, responseCode);
            if (responseCode >= 200 && responseCode <= 299) {
                return response;
            }
            throw new Error(response.toString());
        }
    }

    function Response(result, body, status) {
        this.result = result;
        this.body = body;
        this.status = status;
    }
    Response.prototype.toString = function () {
        return this.body;
    }

    function build(discovery, collection, rootUrl, servicePath, standardParameters, accessToken) {
        for (const name in discovery.resources) {
            const resource = discovery.resources[name];
            collection[name] = build(resource, {}, rootUrl, servicePath, standardParameters, accessToken);
        }
        for (const name in discovery.methods) {
            const method = discovery.methods[name];
            collection[name] = createNewMethod(method, rootUrl, servicePath, standardParameters, accessToken);
        }
        return collection;
    }

    function getAccessToken(credentials) {
        if (credentials.accessToken) {
            return credentials.accessToken;
        }
        const tokenResponse = UrlFetchApp.fetch('https://www.googleapis.com/oauth2/v4/token', { method: 'post', contentType: 'application/x-www-form-urlencoded', muteHttpExceptions: true, payload: { client_id: credentials.clientId, client_secret: credentials.clientSecret, refresh_token: credentials.refreshToken, grant_type: 'refresh_token' } });
        const responseCode = tokenResponse.getResponseCode();
        const responseText = tokenResponse.getContentText();
        if (responseCode >= 200 && responseCode <= 299) {
            const accessToken = JSON.parse(responseText)['access_token'];
            return accessToken;
        }
        throw new Error(responseText);
    }
})(GoogleApis || (GoogleApis = {}));

// https://github.com/AzureAD/microsoft-authentication-library-for-js/blob/master/lib/msal-core/src/Utils.ts
class Base64 {
    static encode(input) {
        const keyStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
        let output = "";
        let chr1, chr2, chr3, enc1, enc2, enc3, enc4;
        let i = 0;
        input = this.utf8Encode(input);
        while (i < input.length) {
            chr1 = input.charCodeAt(i++);
            chr2 = input.charCodeAt(i++);
            chr3 = input.charCodeAt(i++);
            enc1 = chr1 >> 2;
            enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
            enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
            enc4 = chr3 & 63;
            if (isNaN(chr2)) {
                enc3 = enc4 = 64;
            }
            else if (isNaN(chr3)) {
                enc4 = 64;
            }
            output = output + keyStr.charAt(enc1) + keyStr.charAt(enc2) + keyStr.charAt(enc3) + keyStr.charAt(enc4);
        }
        return output.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
    }
    static utf8Encode(input) {
        input = input.replace(/\r\n/g, "\n");
        let utftext = "";
        for (let n = 0; n < input.length; n++) {
            const c = input.charCodeAt(n);
            if (c < 128) {
                utftext += String.fromCharCode(c);
            }
            else if ((c > 127) && (c < 2048)) {
                utftext += String.fromCharCode((c >> 6) | 192);
                utftext += String.fromCharCode((c & 63) | 128);
            }
            else {
                utftext += String.fromCharCode((c >> 12) | 224);
                utftext += String.fromCharCode(((c >> 6) & 63) | 128);
                utftext += String.fromCharCode((c & 63) | 128);
            }
        }
        return utftext;
    }
}
