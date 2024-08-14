const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [ DATABASE_SCHEMA ],
        whitelist: [ `${DATABASE_SCHEMA}.%` ],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db[ 'user' ][ type ];

            if (objects) {
                for (const [ key, func ] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        const data = await axios
            .get('https://datausa.io/api/data?drilldowns=Nation&measures=Population')
            .then(({ data }) => data);

        // Inserção dos dados no banco
        console.log('\n\n\x1b[7m\x1b[1m----- Inserção -----\x1b[0m')
        const existingData = await db[ DATABASE_SCHEMA ].api_data.findOne({
            doc_id: data.source[ 0 ].annotations.table_id
        });
        if (!existingData) {
            await db[ DATABASE_SCHEMA ].api_data.insert({
                doc_record: data,
                api_name: data.source[ 0 ].name,
                doc_id: data.source[ 0 ].annotations.table_id,
                doc_name: data.source[ 0 ].annotations.dataset_name,
            });
        } else console.log('Os dados já foram inseridos no banco. Ignorando...');

        // Calculo da somatória em memória
        console.log('\n\x1b[7m\x1b[1m---- Em memória ----\x1b[0m')
        const sum = data.data
            .filter((item) => [ 2020, 2019, 2018 ].includes(item[ 'ID Year' ]))
            .reduce((acc, cur) => acc + cur.Population, 0)
        console.log(`Resultado da soma: \x1b[33m${sum}\x1b[0m`)

        // Calculo da somatória usando SELECT no banco
        console.log('\n\x1b[7m\x1b[1m----- No banco -----\x1b[0m')
        await db.query(
            `
            SELECT SUM((entry->>'Population')::int) AS "populationSum"
            FROM (
                SELECT jsonb_array_elements(doc_record->'data') AS entry
                FROM ${DATABASE_SCHEMA}.api_data
            ) AS data
            WHERE entry->>'ID Year' IN ('2020', '2019', '2018');
            `
        ).then((result) => {
            console.log(`Resultado da soma: \x1b[33m${result[ 0 ].populationSum}\x1b[0m\n`)
        })
    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
    db.instance.$pool.end();
})();
