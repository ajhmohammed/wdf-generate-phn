const express = require('express');
const app = express();
const { Pool } = require('pg')
const Keycloak = require('keycloak-backend').Keycloak
const http = require('node:http')
const cron = require('node-cron')
const { Luhn } = require('@evanion/luhn')
const path = require('path')
var fs = require('node:fs')
const logging = require('./logging')
const { v4: uuidv4 } = require('uuid')
const dotenv = require('dotenv')
const getFhirResource = require('./getFhirResource')
const keycloakAuth = require('./keycloakAuth')
dotenv.config()

app.use(express.json());

// Input variables
let phnGroupLimit = 2; //Limit the number of PHN groups a practitioner could have at a time
let resourceProcessLimit = 20; // Limit the number of practitioners processed at a time
let maxPhnPerPoi = 1000000; // Maximum PHN could be generated per POI (HIU Guideline)
let phnPerGroupLimit = 100; // How many PHN a group can have
let createDb = false;
let createTables = false;

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

// Fetching all available practitioners
const getAllPractitioners = async function() {

    logging('Info', `Started fetching all Practitioners`)

    try {

        // const getPractitioner = await getFhirResource("GET", "Practitioner", "", "active=true")
        const getPractitioner = await getFhirResource("GET", "Practitioner", "a45894f4-8406-47a9-bdc3-cb5fdb163922", "")

        // console.log("GETTING PRAC", getPractitioner.response)
        // console.log(getPractitioner.response.identifier[1].value)

        returnArray = []

        if(getPractitioner && getPractitioner.status == true) {

            // getPractitioner.response.entry.forEach(item => {
            // getPractitioner.response.forEach(item => {
                logging('Info', `Adding Practitioner to array. Practitioner ID: ${getPractitioner.response.id}`)

                // let returnResult = "";

                let returnResult = {
                    practitionerId: getPractitioner.response.id,
                    keycloakUUID: getPractitioner.response.identifier[1].value
                }

                returnArray.push(returnResult)

                // return returnArray
                
            // })
        }

        return returnArray;
        

    } catch(err) {
        logging('Error', `Error in fetching Practioners: ${err}`)
        return false;
    }
    
}

// getAllPractitioners().then(data => {
//     console.log(data)
// })

// Extract the Practioners id to whom the PHN groups needs to be generated
const extractPrationerIds = async function() {

    const allPractitionerIds = await getAllPractitioners()

    console.log("All Practitioner ID", allPractitionerIds)

    try {

        if(allPractitionerIds) {
            
            logging('Info', `Started Identifiying Practitioners need PHN group to be generated`)

            let newIndex = 0;

            const promises = allPractitionerIds.map(async (item, index) => {

                logging('Info', `Fetching Groups for Practitioner resource: ${item.practitionerId}`)

                //search for active=true is not supported by fhir server
                const getGroupBundle = await getFhirResource("GET", "Group", "", `managing-entity=Practitioner/${item.practitionerId}&type=device`)

                logging('Info', `Started Querying for PractionerDetails for Practitioner ID: ${item.practitionerId}`)
                
                // Fetch PractitionerDetail
                const getPractitionerDetail = await getFhirResource("GET", "PractitionerDetail", "", `keycloak-uuid=${item.keycloakUUID}`)
                    
                if(getGroupBundle && getPractitionerDetail && getPractitionerDetail.status == true) {
                    
                    let phnGroupCounts = 0;

                    // if the status is true, then fetch each entry and get the status count
                    if(getGroupBundle.status == true) {

                        getGroupBundle.response.entry.forEach((groupStatusCheck) => {

                            if(groupStatusCheck.resource.active == true) {
                                phnGroupCounts++
                            }

                        })

                    }
                    
                    if (getGroupBundle.status == false) {
                        phnGroupCounts = 0;
                    }
                    
                    if(phnGroupCounts < phnGroupLimit) {

                        newIndex++;

                        const careteams = getPractitionerDetail.response.entry[0].resource.fhir.careteams
                            ? getPractitionerDetail.response.entry[0].resource.fhir.careteams : undefined
                        const locations = getPractitionerDetail.response.entry[0].resource.fhir.locations
                            ? getPractitionerDetail.response.entry[0].resource.fhir.locations : undefined
                        const teams = getPractitionerDetail.response.entry[0].resource.fhir.teams
                            ? getPractitionerDetail.response.entry[0].resource.fhir.teams : undefined

                        if(careteams !== undefined && locations !== undefined && teams !== undefined) {

                            const careteamId = careteams[0].id
                            const locationId = locations[0].id
                            const teamId = teams[0].id

                            // console.log(index)
                            // console.log(newIndex)
                            // console.log("Practitioner", item.practitionerId)
                            // console.log("KeycloakUUID", item.keycloakUUID)
                            // console.log("phnGroupCounts", phnGroupCounts)
                            // console.log("PractitionerDetail", getPractitionerDetail)
                            // console.log("PD_DETAILS", `CareTeam: ${careteamId} Location: ${locationId} Teams: ${teamId}`)

                            let returnResult = "";

                            if (newIndex < resourceProcessLimit && phnGroupCounts < phnGroupLimit) {

                                returnResult = {
                                    practitionerId: item.practitionerId,
                                    phnGroupCount: phnGroupCounts,
                                    careTeamId: careteamId,
                                    locationId: locationId,
                                    teamId: teamId,
                                    // index: index
                                }

                                return returnResult;

                            }
                        }
                    }
                
                }

            })

            const results = await Promise.all(promises)
            const returnedResults = results.filter(item => item !== undefined)

            return returnedResults

        } else {
            logging('Info', `No resources fetched from Practitioner Function`)
            return false;
        }

    } catch(err) {
        logging('Error', `Error in getting funtion return for practitioners: ${err}`)
        return false;
    }
}

// Get Point of Issue 
const getAvailablePoi = async function () {

    if(createDb) {

        const createDb = new Pool({
            user: process.env.DB_USER,
            host: process.env.DB_HOST,
            password: process.env.DB_PASSWORD,
            port: process.env.DB_PORT,
        });

        const checkDb = await createDb.connect()

        // Create Database if not exists
        try {

            const checkDbExists = await checkDb.query(
                `SELECT 1 FROM pg_database WHERE datname = $1`, 
                [process.env.DB_DATABASE]
            )

            if(checkDbExists.rowCount === 0) {
                await checkDb.query(`CREATE DATABASE ${process.env.DB_DATABASE};`)
                logging("Info", `Created Database: ${process.env.DB_DATABASE}`)
            }

        } catch (error) {
            logging("Error", `Error in creating Database ${error}`)
        } finally {
            checkDb.release();
        }

    }

    // if(createTables) {

    //     const pool = await pool.connect()

    //     // Create tables if not exists (if enabled)
    //     try {

    //         await pool.query(
    //             `CREATE TABLE IF NOT EXISTS poi (
    //                 id SERIAL PRIMARY KEY, 
    //                 poi_number character varying(4) NOT NULL UNIQUE, 
    //                 status character varying(20) NOT NULL,
    //                 total_generated_phn_count integer NOT NULL,
    //                 last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL);`
    //         )

    //         logging("Info", `Created or skipped table poi`)

    //     } catch (error) {
    //         logging("Error", `Error in creating Database poi: ${error}`)
    //     }

    //     try {

    //         await pool.query(
    //             `CREATE TABLE IF NOT EXISTS phn (
    //                 id SERIAL PRIMARY KEY, 
    //                 generated_phn character varying(11) NOT NULL UNIQUE, 
    //                 generated_for character varying(100),
    //                 generated_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL);`
    //         )

    //         logging("Info", `Created or skipped table phn`)

    //     } catch (error) {
    //         logging("Error", `Error in creating Database phn: ${error}`)
    //     } finally {
    //         pool.release();
    //     }

    // }

    // const connection = await pool.connect()

    // check for poi
    try {

        // const query = `SELECT * FROM poi WHERE status = 'active' AND total_generated_phn_count < ${maxPhnPerPoi} ORDER BY id ASC`;
        // const { rows } = await connection.query(query);


        if(rows.length > 0) {

            logging("Info", `Fetched POI: ${rows[0]['poi_number']}, status: ${rows[0]['status']}, totalPhnGenerated: ${rows[0]['total_generated_phn_count']}`)

            response = {
                response: {
                    poiNumber: rows[0]['poi_number'],
                    status: rows[0]['status'],
                    totalPhnGenerated: rows[0]['total_generated_phn_count']
                },
                status: true
            }

        } else {

            logging("Info", `No POIs fetched from the db`)

            response = {
                status: false
            }            

        }

        return response;

    } catch (error) {
        logging("Error", `Failed to fetch poi: ${error}`)
    } finally {
        connection.release()
    }

}

// Generate PHN Number
const generateUniquePhn = async function() {

    logging('Info', `Started generating unique PHN`)

    // const poiResponse = await getAvailablePoi(maxPhnPerPoi);
    const poiResponse = true;

    // if(poiResponse && poiResponse.status == true) {
    if(poiResponse) {

        // poi = poiResponse.response.poiNumber
        poi = "3053"
        // totalPhnGenerated = poiResponse.response.totalPhnGenerated

        logging('Info', `Returned POI Info - POI Number: ${poi}`)

        class MyLuhn extends Luhn {
            static dictionary = "234567890BCDFGHJKMPQRTVWXY"
            static sensitive = true
        }

        const generateRandomString = function(length) {
            const chars = "2346789BCDFGHJKMPQRTVWXY";
            let result = "";
        
            for (let i = 0; i < length; i++) {
                result += chars.charAt(Math.floor(Math.random() * chars.length));
            }
            
            return result;
        }

        const randomString = generateRandomString(6)
        const initalPhn = poi + randomString
        const checksum = MyLuhn.generate(initalPhn)
        const validated = MyLuhn.validate((checksum.phrase + checksum.checksum).toUpperCase())

        logging('Info', `Generated PHN String - randomString: ${randomString}, intialPhn: ${initalPhn}, checksum: ${checksum.checksum}, finalPhn: ${validated.phrase}, validation: ${validated.isValid}`)

        let returnResult = "";

        returnResult = {
            poi: poi,
            // totalPhnGenerated: totalPhnGenerated,
            randomString: randomString,
            initalPhn: initalPhn,
            checksum: checksum,
            validation: validated
        }

        return returnResult;    

    }

}

// Get PHN Array ready based on phnPerGroupLimit
const generatePhnArray = async function() {

    // Intialize return array
    let returnArray = []

    // loop through the number of phns needs to be generated per group
    while(returnArray.length < phnPerGroupLimit) {

        const getPhn = await generateUniquePhn()

        let poi = getPhn.poi
        // let totalPhnGenerated = getPhn.totalPhnGenerated
        let phn = getPhn.validation.phrase
        let isValid = getPhn.validation.isValid

        // Insert into db (if generated phn already exist, this should fail)
        // const connection = await pool.connect();

        try {

            // const InsertPhn = `INSERT INTO phn (generated_phn) VALUES ($1);`;
            // const insertResult = await connection.query(InsertPhn, [phn])

            // if(insertResult.rowCount == 1) {
            if(1 == 1) {

                logging('Info', `PHN: ${phn} successfully inserted`)

                // If db insertion success, then increment the generatedPhn count
                // totalPhnGenerated++

                // update the db for the count if the totalPhnGenerated is < max value
                // if (totalPhnGenerated < maxPhnPerPoi) {

                //     const updatePoiCount = `UPDATE poi SET total_generated_phn_Count = $1 WHERE poi_number = $2;`;
                //     const updatePoiCountResult = await connection.query(updatePoiCount, [`${totalPhnGenerated}`, `${poi}`])

                //     if(updatePoiCountResult.rowCount == 1) {

                //         logging('Info', `Incremented the POI Generated count`)

                //     }

                // // update the db and change status to inactive if the totalPhnGenerated is not < max value
                // } else {

                //     const updatePoiStatus = `UPDATE poi SET status = $1, total_generated_phn_Count = $2 WHERE poi_number = $3;`;
                //     const updatePoiStatusResult = await connection.query(updatePoiStatus, [`inactive`,`${totalPhnGenerated}`, `${poi}`])

                //     if(updatePoiStatusResult.rowCount == 1) {

                //         logging('Info', `Update the Total phn generated and marked the status inactive`)

                //     }

                // }

                // push to array
                response = {
                    "code": {
                        "text": "phn"
                    }, 
                    "valueCodeableConcept": {
                        "text": phn
                    },
                    "exclude": false
                }
        
                returnArray.push(response)

            }

        } catch (error) {

            logging('Error', `Something went wrong in query: ${error}`)

        } finally {
            // connection.release()
        }

    }

    return returnArray
}



// Create Bundle Array and post bundle
const postPhnBundle = async function() {

    let entry = [];

    logging('Info', `Getting Practioners from function`)

    // Fetch all the practioners need phn
    const fetchedPractitioners = await extractPrationerIds();

    console.log(fetchedPractitioners);

    if(fetchedPractitioners && fetchedPractitioners.length > 0) {

        for await (const practitioner of fetchedPractitioners) {

            practitionerId = practitioner.practitionerId
            phnGroupCount = practitioner.phnGroupCount
            careTeamId = practitioner.careTeamId
            locationId = practitioner.locationId
            teamId = practitioner.teamId

            // Count to identify how many phn groups to be generated
            for (i = phnGroupCount; i < phnGroupLimit; i++) {

                // Generate the PHN array (Build the PHN array)
                const generatePhn = await generatePhnArray();

                let groupUUID = uuidv4();

                logging('Info', `Generating Group: ${groupUUID} for ${practitionerId}`)

                phnArray = {
                    "fullUrl": process.env.HAPI_BASE_URL_BUNDLE+`/Group/${groupUUID}`,
                    "resource": {
                        "resourceType": "Group",
                        "id": `${groupUUID}`,
                        "meta": {
                            "tag": [
                                {
                                    "system": "https://smartregister.org/care-team-tag-id",
                                    "code": `${careTeamId}`,
                                    "display": "Practitioner CareTeam"
                                },
                                {
                                    "system": "https://smartregister.org/location-tag-id",
                                    "code": `${locationId}`,
                                    "display": "Practitioner Location"
                                },
                                {
                                    "system": "https://smartregister.org/organisation-tag-id",
                                    "code": `${teamId}`,
                                    "display": "Practitioner Organization"
                                },
                                {
                                    "system": "https://smartregister.org/practitioner-tag-id",
                                    "code": `${practitionerId}`,
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
                            "reference": `Practitioner/${practitionerId}`,
                        },
                        "characteristic": generatePhn,
                    },
                    "search": {
                        "mode": "match"
                    },
                    "request": {
                        "method": "PUT",
                        "url": `Group/${groupUUID}`
                    }
                }

                entry.push(phnArray)

            }       

        }

        // return entry

        // push in to bundle 
        var bundleUUID = uuidv4();

        logging('Info', `Generating Bundle: ${bundleUUID}`)

        let bundleOutput = {
            "resourceType": "Bundle",
            "id": `${bundleUUID}`,
            "type": "transaction",
            "entry": entry
        }

        // return bundleOutput

        // post the bundle

        const accessToken = await keycloakAuth.accessToken.get();

        const options = {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                Authorization: ` Bearer ${accessToken}`
            },
            body: JSON.stringify(bundleOutput)
        }

        const url = process.env.HAPI_BASE_URL;

        try {
            logging('Info', `Posting Bundle: ${bundleUUID}`)

            const response = await fetch(url, options)
            const jsonResponse = await response.json();
            console.log(jsonResponse);

        } catch(err) {
            logging('Error', `Error in posting Bundle: ${bundleUUID}. ERROR: ${err}`)
        }

        // return postBundle
    } else {
        logging('Info', `No bundles to post`)
    }


}

// getAllPractitioners()
    
// generatePhnArray().then(data => {
//     console.log(data)
// })


// function logMessage() {
//     logMessage = `Cron job executed at:, ${new Date().toLocaleString()} \n`
//     fs.appendFile(logFileName, logMessage, function (err) {})    
// }

// cron.schedule('*/2 * * * *', () => {
//     logging('CRON', `CRON JOB EXECUTED`)
    
//     postPhnBundle()
// });

// cron.schedule('* * * * *', () => {
//     logMessage = `LIVE: NODE service live, ${new Date().toLocaleString()} \n`
//     fs.appendFile(logFileName, logMessage, function (err) {})
// });

app.get('/generatePhn', (req, res) => {   

    logging('CRON', `CRON JOB EXECUTED`)

    postPhnBundle().then(x => {
        // Send the sms
    })

    res.send('Running');    
    
})

postPhnBundle().then(x => {
    // Send the sms
})


app.listen(process.env.PORT, () => {
    (process.env.NODE_ENV !== 'prod') ? console.log(`Listening on port ${process.env.PORT}`): ''
})