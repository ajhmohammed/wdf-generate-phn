var fs = require('node:fs')
const path = require('node:path')

const logging = (logLevel, message) => {

    // Create a seperate folder for logging
    const folderName = 'logs'

    if (!fs.existsSync(folderName)) {
        fs.mkdir(folderName, {recursive:true}, (err) => {
            if(err) {
                console.error(`Error appending to file: ${err}`);
            } else {
                console.log('Content appended successfully.');
                appendToFile();
            }
        });
    } else {
        appendToFile();
    }

    function appendToFile() {

        //Log filename based on the date
        let today = new Date();
        let year = today.getFullYear()
        let month = String(today.getMonth() + 1).padStart(2, '0')
        let date = String(today.getDate()).padStart(2, '0')

        logFileName = `logs_${year+month+date}.txt`

        // Date time
        logTime = new Date().toLocaleString();

        // log level [Error, Info, warn]
        logLevel = "[" + logLevel + "]";

        // Message
        message = message; 

        logMessage = logTime + "\t" + logLevel + "\t" + message + "\n";

        console.log(logMessage);

        fs.appendFile(`${folderName}/${logFileName}`, logMessage, function (err) {})

    }
}

module.exports = logging