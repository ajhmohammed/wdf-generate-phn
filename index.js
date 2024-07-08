const express = require('express')
const { Pool } = require('pg')
const Keycloak = require('keycloak-backend').Keycloak
const http = require('node:http')
const cron = require('node-cron')
const { Luhn } = require('@evanion/luhn')
const path = require('path')
const { copyFileSync } = require('node:fs')
const { v4: uuidv4 } = require('uuid')
const dotenv = require('dotenv')
const app = express()
dotenv.config()

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
        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Started the query for practitioner count`)

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        // console.log(`Total No. of Practitioners: ${jsonResponse.total}`);

        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Total number of Practitioners: ${jsonResponse.total}`)

        return (jsonResponse.total);

        // return 5;

    } catch(err) {
        console.log('ERROR ', err);
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

        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Started fetching all Practitioners`)

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        // console.log(jsonResponse.entry.);
        practitionerJsonArray.push(jsonResponse.entry);
        // console.log(practitionerJsonArray[0][0]);

        // var a = 1;

        for(const practitionerIds of practitionerJsonArray[0]) {
            // console.log(a++);

            const thisPractitionerId = practitionerIds.resource.id;

            console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Fetched Practitioner: ${thisPractitionerId}`)

            practitionerIdListArray.push(thisPractitionerId);
            

            // getPractitionerPhnGroupCount().then(x => {
                // console.log(a++, thisPractitionerId, getPractitionerPhnGroupCount());
            // })

            // console.log(a++, thisPractitionerId, getPractitionerPhnGroupCount());
            // console.log(practitionerIds.resource.id);
        }

       

        return practitionerIdListArray;

        // return (jsonResponse.total);
    } catch(err) {
        console.log('ERROR ', err);
    }
}

// Get the list of practitioners for whom the PHN group needs to be generated
const getPractitionerList = async function(req, res) {

    const allPractitionerIds = await getAllPractitionersId();

    var a = 0;
    let finalPractitionerList = [];

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Started identifying the Practitioners need to generate the PHN Group`)

    for await (const practitionerIds of allPractitionerIds) {

        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Checking Practitioner: ${practitionerIds}`)

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
                console.log('ERROR ', err);
            }

            
        }

        const getGroupCounts = await getGroupCount();

        if(getGroupCounts < 2) {
            finalPractitionerList.push({"id": practitionerIds, "count": await getGroupCount()});
        }

    }

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Ended identifying the list of Practitioners need PHN Group(s)`)

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
            console.log(`PROCESS: \t ${new Date().toLocaleString()} \t No active poi(s) are available on the database`)
            process.exit(1);
        } else {
            console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Obtained available pois`)

            return rows[0]['poinumber'];
        }

    } catch (err) {
        console.error(err);
        return 'failed to fetch poi(s)';
    }
}

async function generatePhn() {

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Generating PHNs`)

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

    // console.log('POI: ' + poi);
    // console.log('Random String: ' + randomString);
    // console.log('Inital PHN: ' + initialPhn);
    // console.log('Luhn Output: ', luhnCheck);
    // console.log('Checksum: ' + checksum);
    // console.log('Final PHN: ' + finalPhn);
    // console.log('Final PHN Valid: ', finalPhnValidation);

    // if (finalPhnValidation.isValid === true) {
        // return finalPhnValidation;
    // }

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

        console.log("Generated PHN: " + generatedPhnPhrase);

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

    const UpdateGeneratedPhnCount =   `
        UPDATE pois SET phnGenerated = phnGenerated + 100;
    `;

    const result = await pool.query(UpdateGeneratedPhnCount);

    // let phnArrayListStringy = JSON.stringify(phnArrayList)
    // let finalphnArrayListStringy = phnArrayListStringy.substring(1, phnArrayListStringy.length-1)
    // return JSON.stringify(finalphnArrayListStringy);

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Generated the PHN Group(s)`)

    return phnArrayList;
}

// Get the PractitionerDetails
async function fetchPractitionerDetailResource() {

    const practitionersList = await getPractitionerList();

    let practitionerMetaInfo = [];

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Started fetching PractitionerDetail resource`)

    for await (const practList of practitionersList) {

        const practitionerId = practList.id;
        const phnGroupCount = practList.count;

        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Fetching PractitionerDetail for ${practitionerId}`)

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
                // console.log(`Practitioner Id: ${practitionerId}`)
                // console.log(`\t CareTeam: ${careTeams[0].id}`)
                // console.log(`\t Location: ${locations[0].id}`)
                // console.log(`\t Team: ${teams[0].id}`)

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

                // return practitionerMetaInfo;

                // console.log(practitionerMetaInfo)

            }
            
        } catch(err) {
            console.log('ERROR ', err);
        }

    }

    console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Fetched PractitionerDetail Information`)

    return practitionerMetaInfo;

}


// Performance evaluation
async function generatePhnBundle() {

    const practionerDetails = await fetchPractitionerDetailResource();

    let entry = [];

    for await (const pdInfo of practionerDetails) {
    
        const phnGroupCount = pdInfo.phnGroupCount;

        /*
            async function generateIndividualBundle() {

                var groupUUID = uuidv4();
                let bundleArray = "";

                bundleArray =
                    {
                        "fullUrl": `http://188.166.213.172:9002/fhir/Group/${groupUUID}`,
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
                            "quantity": 100,
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

            //    return JSON.stringify(bundleArray);
            return bundleArray;
            }
        */

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
            // const individualBundle = await generateIndividualBundle();
            // // console.log(individualBundle);

            // // let individualBundleStringy = JSON.stringify(individualBundle)
            // // let individualBundleStringyFinal = (individualBundleStringy.substring(1, individualBundleStringy.length-1));

            // bundleArray.push(individualBundle)

            
            
        }

        // console.log(entry);

    }

        var bundleUUID = uuidv4();

        let bundleOutput = {
            "resourceType": "Bundle",
            "id": `${bundleUUID}`,
            "type": "transaction",
            "entry": entry
        }

        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Generated the bundle succesfully`)

        return bundleOutput
}

// generatePhnBundle();

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
        console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Started the query for practitioner count`)

        const response = await fetch(url, options)
        const jsonResponse = await response.json();
        console.log(jsonResponse);

        // console.log(`PROCESS: \t ${new Date().toLocaleString()} \t Total number of Practitioners: ${jsonResponse.total}`)

        // return (jsonResponse.total);

        // return 5;

    } catch(err) {
        console.log('ERROR ', err);
    }

    // return JSON.stringify(phnBundle);
    // return phnBundle;
}


// app.use(express.json());


// app.get('/', (req, res) => {

//     postBundle();

//     res.send('loaded');   

//     // getPractitioners()
//     // res.status(200).json(getPractitioners());
//     // req.write('data\n');
//     // req.write('data\n');
//     // req.end();
// })

// postBundle();

function logMessage() {
    console.log('Cron job executed at:', new Date().toLocaleString());

    postBundle();

    // getPractitionerCount().then(x => {
    //     console.log(x);
    // })
}

cron.schedule('*/2 * * * *', () => {
    logMessage();
});

app.listen(process.env.PORT, () => {
    (process.env.NODE_ENV !== 'prod') ? console.log(`Listening on port ${process.env.PORT}`): ''
})