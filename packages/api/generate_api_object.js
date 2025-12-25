const OpenAPI = require('../../node_modules/openapi-typescript-codegen');
const fs = require('fs');
const path = require('path');

async function run() {
    try {
        const apiSpec = JSON.parse(fs.readFileSync(path.resolve('./src/clients/trading/api.json'), 'utf8'));
        console.log('Successfully read API spec');

        await OpenAPI.generate({
            input: apiSpec,
            output: './src/clients/trading/__generated__',
            exportServices: true,
            exportModels: true,
            useOptions: true
        });
        console.log('Generation completed successfully!');
    } catch (error) {
        console.error('Generation failed:', error);
        process.exit(1);
    }
}

run();
