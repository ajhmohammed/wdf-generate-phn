const Keycloak = require('keycloak-backend').Keycloak
const dotenv = require('dotenv')
dotenv.config();

const keycloakAuth = new Keycloak({
    "realm": process.env.KC_REALM,
    "keycloak_base_url": process.env.KC_BASE_URL,
    "client_id": process.env.KC_CLIENT_ID,
    "username": process.env.KC_USERNAME,
    "password": process.env.KC_PASSWORD,        
    "is_legacy_endpoint": process.env.KC_IS_LEGACY_ENDPOINT
});

module.exports = keycloakAuth