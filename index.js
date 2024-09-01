const express = require('express')
const app = express()
const { Pool } = require('pg')
const Keycloak = require('keycloak-backend').Keycloak
const http = require('node:http')
const cron = require('node-cron')
const { Luhn } = require('@evanion/luhn')
const path = require('path')
var fs = require('node:fs')
const { v4: uuidv4 } = require('uuid')
const dotenv = require('dotenv')

dotenv.config()

app.use(express.json());

const logFileName = 'logs.txt'
var logMessage = `Started Node app ${new Date().toLocaleString()} \n`;

fs.appendFile(logFileName, logMessage, function (err) {})

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

const keycloak = new Keycloak({
    "realm": process.env.KC_REALM,
    "keycloak_base_url": process.env.KC_BASE_URL,
    "client_id": process.env.KC_CLIENT_ID,
    "username": process.env.KC_USERNAME,
    "password": process.env.KC_PASSWORD,        
    "is_legacy_endpoint": process.env.KC_IS_LEGACY_ENDPOINT
});


// Function to get the total number of practitioners
const getPractitionerCount = async function(req, res) {

    const accessToken = await keycloak.accessToken.get();

    const options = {
        method: 'GET',
        headers: {
            Authorization: ` Bearer ${accessToken}`
        }
    }

    const url = process.env.HAPI_BASE_URL+'/Practitioner/?_summary=count';

    try {
        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Started the query for practitioner count \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        // console.log(`Total No. of Practitioners: ${jsonResponse.total}`);

        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Total number of Practitioners: ${jsonResponse.total} \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        return (jsonResponse.total);

        // return 5;

    } catch(err) {
        // console.log('ERROR ', err);
        logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err \n`
        fs.appendFile(logFileName, logMessage, function (err) {})
    }

    
}

// getPractitionerCount();

const getAllPractitionersId = async function(req, res) {

    const accessToken = await keycloak.accessToken.get();

    const options = {
        method: 'GET',
        headers: {
            Authorization: ` Bearer ${accessToken}`
        }
    }

    const totalPractitionerCount = await getPractitionerCount();
    
    const url = process.env.HAPI_BASE_URL+`/Practitioner?_count=${totalPractitionerCount}`;

    let practitionerJsonArray = [];
    let practitionerIdListArray = [];

    try {

        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Started fetching all Practitioners \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        // console.log(jsonResponse.entry.);
        practitionerJsonArray.push(jsonResponse.entry);
        // console.log(practitionerJsonArray[0][0]);

        // var a = 1;

        for(const practitionerIds of practitionerJsonArray[0]) {
            // console.log(a++);

            const thisPractitionerId = practitionerIds.resource.id;

            logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Fetched Practitioner: ${thisPractitionerId} \n`
            fs.appendFile(logFileName, logMessage, function (err) {})

            practitionerIdListArray.push(thisPractitionerId);
            
        }       

        return practitionerIdListArray;

        // return (jsonResponse.total);
    } catch(err) {
        logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err \n`
        fs.appendFile(logFileName, logMessage, function (err) {})
    }
}

// Get the list of practitioners for whom the PHN group needs to be generated
const getPractitionerList = async function(req, res) {

    const allPractitionerIds = await getAllPractitionersId();

    var a = 0;
    let finalPractitionerList = [];

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Started identifying the Practitioners need to generate the PHN Group \n`
    fs.appendFile(logFileName, logMessage, function (err) {})

    for await (const practitionerIds of allPractitionerIds) {

        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Checking Practitioner: ${practitionerIds} \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        const getGroupCount = async function(req, res) {

            const accessToken = await keycloak.accessToken.get();

            const options = {
                method: 'GET',
                headers: {
                    Authorization: ` Bearer ${accessToken}`
                }
            }

            // (type: device, status: active)
            const url = process.env.HAPI_BASE_URL+`/Group?managing-entity=Practitioner/${practitionerIds}&type=device`;

            let practitionerGroupJsonArray = [];

            try {

                const response = await fetch(url, options)
                const jsonResponse = await response.json();

                if (jsonResponse.entry !== undefined) {

                    // Check the status is active
                    practitionerGroupJsonArray.push(jsonResponse.entry);

                    // if the returned result has more than one group
                    if (practitionerGroupJsonArray[0].length > 1) {

                        let groupActiveCount = 0;
                        for(const groupJsonsArr of practitionerGroupJsonArray[0]) {
                            if(groupJsonsArr.resource.active === true) {
                                groupActiveCount++;
                            }
                        }

                        return groupActiveCount;

                    } else {
                        if (practitionerGroupJsonArray[0][0].resource.active === true) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }

                } else {
                    return 0;
                }
    
            } catch(err) {
                logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err \n`
                fs.appendFile(logFileName, logMessage, function (err) {})
            }

            
        }

        const getGroupCounts = await getGroupCount();

        if(getGroupCounts < 2) {
            finalPractitionerList.push({"id": practitionerIds, "count": await getGroupCount()});
        }

    }

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Ended identifying the list of Practitioners need PHN Group(s) \n`
    fs.appendFile(logFileName, logMessage, function (err) {})

    return finalPractitionerList;   

}

// @TODO Insert status column for the poi
// @TODO fetch one record from one poi
// @TODO dynamic phn group (count)
// @TODO PHN Count limit
async function getAvailablePoi() {

    try {
        const query = 'SELECT * FROM pois';
        const { rows } = await pool.query(query);
        
        if(rows.length === 0) {
            // return 'No active poi(s) are available on the database.';
            logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t No active poi(s) are available on the database \n`
            fs.appendFile(logFileName, logMessage, function (err) {})

            process.exit(1);
        } else {
            logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Obtained available pois \n`
            fs.appendFile(logFileName, logMessage, function (err) {})

            return rows[0]['poinumber'];
        }

    } catch (err) {
        // console.error(err);
        // return 'failed to fetch poi(s)';
        logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err failed to fetch pois \n`
        fs.appendFile(logFileName, logMessage, function (err) {})
    }
}

async function generatePhn() {

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Generating PHNs \n`
    fs.appendFile(logFileName, logMessage, function (err) {})

    const poi = await getAvailablePoi();

    const generateRandomString = function(length) {
        const chars = "2346789BCDFGHJKMPQRTVWXY";
        let result = "";
    
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        
        return result;
    }

    const randomString = generateRandomString(6);
    const initialPhn = (poi + randomString).toUpperCase();
    const luhnCheck = Luhn.generate(initialPhn, false);
    const checksum =  luhnCheck.checksum;  

    const finalPhn = (initialPhn + checksum).toUpperCase();
    const finalPhnValidation = Luhn.validate(finalPhn);

    const chars = "2346789BCDFGHJKMPQRTVWXY";

    if (chars.includes(checksum.toUpperCase())) {
        // console.log("YES");

        return finalPhnValidation;
    } else {
        // console.log("NO");

        finalPhnValidation.isValid = false;
        
        return finalPhnValidation;
    }    
    
}

async function generatePhnArray() {

    let phnArrayList = [];

    let ab = 0;

    while(phnArrayList.length <= 99){
    // while(phnArrayList.length <= 5){

        const generatedPhn = await generatePhn();
        const generatedPhnPhrase = generatedPhn.phrase.toUpperCase()

        logMessage = `Generated PHN:  + ${generatedPhnPhrase}  \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        if(generatedPhn.isValid == true) {

            // Insert the PHN into the db and update the count at the end
            const InsertPhns =   `
                INSERT INTO phns (phn)
                VALUES ($1)
                RETURNING id;
            `;

            const phns = [generatedPhnPhrase];
        
            const result = await pool.query(InsertPhns, phns);

            if(result.rowCount == 1) {

                phnArrayList.push(
                    {
                        "code": {
                            "text": "phn"
                        }, 
                        "valueCodeableConcept": {
                            "text": generatedPhnPhrase
                        },
                        "exclude": false
                    }
                ); 

            }

        }   
    
    }

    const UpdateGeneratedPhnCount = `UPDATE pois SET phnGenerated = phnGenerated + 100;`;

    const result = await pool.query(UpdateGeneratedPhnCount);

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Generated the PHN Group(s) \n`
    fs.appendFile(logFileName, logMessage, function (err) {})

    return phnArrayList;
}

// Get the PractitionerDetails
async function fetchPractitionerDetailResource() {

    const practitionersList = await getPractitionerList();

    let practitionerMetaInfo = [];

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Started fetching PractitionerDetail resource \n`
    fs.appendFile(logFileName, logMessage, function (err) {})
    
    for await (const practList of practitionersList) {

        const practitionerId = practList.id;
        const phnGroupCount = practList.count;

        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Fetching PractitionerDetail for ${practitionerId} \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        const accessToken = await keycloak.accessToken.get();

        const options = {
            method: 'GET',
            headers: {
                Authorization: ` Bearer ${accessToken}`
            }
        }

        const url = process.env.HAPI_BASE_URL+`/PractitionerDetail?keycloak-uuid=${practitionerId}`;

        try {

            const response = await fetch(url, options)
            const jsonResponse = await response.json();

            const careTeams = jsonResponse.entry[0].resource.fhir.careteams;
            const locations = jsonResponse.entry[0].resource.fhir.locations;
            const teams = jsonResponse.entry[0].resource.fhir.teams;

            if(careTeams !== undefined && locations !== undefined && teams !== undefined) {
                
                const careTeamId = careTeams[0].id;
                const locationId = locations[0].id;
                const teamId = teams[0].id;
                const applicationVersion = `Not defined`;                

                practitionerMetaInfo.push({
                    "practitionerId": practitionerId, 
                    "careTeamId": careTeamId, 
                    "locationId": locationId, 
                    "teamId": teamId, 
                    "applicationVersion": applicationVersion,
                    "phnGroupCount": phnGroupCount
                })

            }
            
        } catch(err) {
            logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err \n`
            fs.appendFile(logFileName, logMessage, function (err) {})
        }

    }

    logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Fetched PractitionerDetail Information  \n`
    fs.appendFile(logFileName, logMessage, function (err) {})

    return practitionerMetaInfo;

}


// Performance evaluation
async function generatePhnBundle() {

    const practionerDetails = await fetchPractitionerDetailResource();

    let entry = [];

    for await (const pdInfo of practionerDetails) {
    
        const phnGroupCount = pdInfo.phnGroupCount;

        // Count to identify how many phn groups to be generated
        for (i = phnGroupCount; i < 2; i++) {

            const getPhnArry = await generatePhnArray();

            var groupUUID = uuidv4();
            let bundleArray = "";

            bundleArray =
                {
                    "fullUrl": process.env.HAPI_BASE_URL_BUNDLE+`/Group/${groupUUID}`,
                    "resource": {
                        "resourceType": "Group",
                        "id": `${groupUUID}`,
                        "meta": {
                            "tag": [
                                {
                                    "system": "https://smartregister.org/care-team-tag-id",
                                    "code": `${pdInfo.careTeamId}`,
                                    "display": "Practitioner CareTeam"
                                },
                                {
                                    "system": "https://smartregister.org/location-tag-id",
                                    "code": `${pdInfo.locationId}`,
                                    "display": "Practitioner Location"
                                },
                                {
                                    "system": "https://smartregister.org/organisation-tag-id",
                                    "code": `${pdInfo.teamId}`,
                                    "display": "Practitioner Organization"
                                },
                                {
                                    "system": "https://smartregister.org/app-version",
                                    "code": `${pdInfo.applicationVersion}`,
                                    "display": "Application Version"
                                },
                                {
                                    "system": "https://smartregister.org/practitioner-tag-id",
                                    "code": `${pdInfo.practitionerId}`,
                                    "display": "Practitioner"
                                }
                            ]
                        },
                        "identifier": [
                            {
                                "system": "http://smartregister.org",
                                "value": `${groupUUID}`
                            }
                        ],
                        "active": true,
                        "type": "device",
                        "actual": true,
                        "name": "Unique IDs",
                        "quantity": 0,
                        "managingEntity": {
                            "reference": `Practitioner/${pdInfo.practitionerId}`,
                        },
                        "characteristic": getPhnArry,
                    },
                    "search": {
                        "mode": "match"
                    },
                    "request": {
                        "method": "PUT",
                        "url": `Group/${groupUUID}`
                    }
                }

            entry.push(bundleArray)
            
        }

    }

        var bundleUUID = uuidv4();

        let bundleOutput = {
            "resourceType": "Bundle",
            "id": `${bundleUUID}`,
            "type": "transaction",
            "entry": entry
        }

        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Generated the bundle succesfully \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        return bundleOutput
}

// Post the bundle to the server
const postBundle = async function(req, res) {

    const accessToken = await keycloak.accessToken.get();
    const phnBundle = await generatePhnBundle();
    
    const options = {
        method: 'POST',
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            Authorization: ` Bearer ${accessToken}`
        },
        body: JSON.stringify(phnBundle)
    }

    const url = process.env.HAPI_BASE_URL;

    try {
        logMessage = `PROCESS: \t ${new Date().toLocaleString()} \t Started the query for practitioner count \n`
        fs.appendFile(logFileName, logMessage, function (err) {})

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        console.log(jsonResponse);

    } catch(err) {
        logMessage = `ERROR : \t ${new Date().toLocaleString()} \t err \n`
        fs.appendFile(logFileName, logMessage, function (err) {})
    }

}

// postBundle();

function logMessage() {
    logMessage = `Cron job executed at:, ${new Date().toLocaleString()} \n`
    fs.appendFile(logFileName, logMessage, function (err) {})    
}

cron.schedule('*/2 * * * *', () => {
    logMessage();
    postBundle();
});

cron.schedule('* * * * *', () => {
    logMessage = `LIVE: NODE service live, ${new Date().toLocaleString()} \n`
    fs.appendFile(logFileName, logMessage, function (err) {})
});

app.get('/generatePhn', (req, res) => {   

    postBundle().then(x => {
        // Send the sms
    })

    res.send('Running');    
    
})


app.listen(process.env.PORT, () => {
    (process.env.NODE_ENV !== 'prod') ? console.log(`Listening on port ${process.env.PORT}`): ''
})