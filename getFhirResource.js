const logging = require('./logging')
const dotenv = require('dotenv');
const { response } = require('express');
const keycloakAuth = require('./keycloakAuth')
dotenv.config();

const getFhirResource = async (method, fhirResource, resourceId, query, bodyContent) => {

    const accessToken = await keycloakAuth.accessToken.get();

    if(method == "GET") {

        const options = {
            method: 'GET',
            headers: {
                Authorization: ` Bearer ${accessToken}`
            }
        }

        const url = process.env.HAPI_BASE_URL + (fhirResource ? `/${fhirResource}` : '') + (resourceId ? `/${resourceId}` : '') + (query ? `?${query}` : '')

        try {

            logging('Info', `Querying fhir server initiated (Url: ${url})`)

            const urlResponse = await fetch(url, options)
            const jsonResponse = await urlResponse.json();

            //return single resource if you have resource id
            if(resourceId) {

                logging('Info', `Resources returned based on the single resource query. Resource: ${fhirResource}, Resource id: ${resourceId}`)

                if(jsonResponse) {

                    return {
                        response: jsonResponse,
                        status: true
                    }

                } else {

                    logging('Info', `Exitting since there is no resources.`)
                    return {
                        status: false
                    };

                }

            } else {

                logging('Info', `Resources returned based on the query ${jsonResponse.total}`)

                if(jsonResponse.total > 0) {

                    return {
                        response: jsonResponse,
                        status: true
                    }

                } else {

                    logging('Info', `Exitting since there is no resources. Returned resource count: ${jsonResponse.total}`)
                    return {
                        status: false
                    };

                }
            }

        } catch(error) {

            logging('Error', `Unable to query the fhir server. Error: ${error}`)
            return false;

        }

    } else if (method == "PATCH") {

        const options = {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json-patch+json',
                Authorization: ` Bearer ${accessToken}`
            },
            body: bodyContent
        }

        const url = process.env.HAPI_BASE_URL + (fhirResource ? `/${fhirResource}` : '') + (resourceId ? `/${resourceId}` : '') + (query ? `?${query}` : '')

        try {

            logging('Info', `Querying fhir server initiated (Url: ${url})`)

            const urlResponse = await fetch(url, options)
            const jsonResponse = await urlResponse.json();

            //return single resource if you have resource id


            logging('Info', `Updating resource. Resource: ${fhirResource}, Resource id: ${resourceId}`)

            if(jsonResponse) {

                logging('Info', `Successfully updated the resource. Resource: ${fhirResource}, Resource id: ${resourceId}`)

                return {
                    response: jsonResponse,
                    status: true
                }

            } else {

                logging('Info', `Exitting since there is no resources.`)
                return {
                    status: false
                };

            }


        } catch(error) {

            logging('Error', `Unable to query the fhir server. Error: ${error}`)
            return false;

        }

    } else if (method == "POST") {

        const options = {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json-patch+json',
                Authorization: ` Bearer ${accessToken}`
            },
            body: JSON.stringify(bodyContent)
        }

        const url = process.env.HAPI_BASE_URL + (fhirResource ? `/${fhirResource}` : '') + (resourceId ? `/${resourceId}` : '') + (query ? `?${query}` : '')

        try {

            logging('Info', `POSTing to fhir server initiated (Url: ${url})`)

            const urlResponse = await fetch(url, options)
            const jsonResponse = await urlResponse.json();

            //return single resource if you have resource id


            logging('Info', `POSTed resource. Resource: ${fhirResource}, Resource id: ${resourceId}`)

            if(jsonResponse) {

                logging('Info', `Successfully updated the resource. Resource: ${fhirResource}, Resource id: ${resourceId}`)

            } else {

                logging('Info', `Something went wrong in posting`)

            }


        } catch(error) {

            logging('Error', `Unable to POST to the server. Error: ${error}`)
            return false;

        }
    }

}

module.exports = getFhirResource