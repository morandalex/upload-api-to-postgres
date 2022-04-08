//IMPORT STATEMENT
import dotenv from 'dotenv';
import fs from 'fs'
import fetch from 'node-fetch';
import * as pg from 'pg'
const { Pool } = pg.default
dotenv.config();
// CLIENT POSTGRESS CONNECTION
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_POSTGRES,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
    poolSize: 20000
})
//READ QRY FROM FILE
function readSqlQryFromFile(filename) {
    let qry = ''
    try {
        var data = fs.readFileSync(filename, 'utf8');
        qry = data.toString();
    } catch (e) {
        console.log('Error:', e.stack);
    }
    // console.log(qry);
    return qry
}
//INSERT ROWS WITH PARAMETERS
async function insertRows(sqlFile, values) {
    const qry = readSqlQryFromFile(sqlFile)
    try {
        const res = await pool.query(qry, values)
    } catch (err) {
        console.log(err.stack)
    }
}
//GET DATA FROM API SERVICE
function getDataFromOpenApi(arr, atecoCode) {
    const result = []
    let i;
    //  console.log(JSON.stringify(arr))
    var d = new Date();
    d.toLocaleString();
    for (i = 0; i < arr.length; i++) {
        if (arr[i].piva != '' || arr[i].piva != null) {
            result.push(
                {
                    lat: String(parseFloat(String(arr[i].gps.coordinates).split(',')[0])),
                    long: String(parseFloat(String(arr[i].gps.coordinates).split(',')[1])),
                    cf: arr[i].cf,
                    ragsoc: arr[i].denominazione.replace(/[^a-zA-Z ]/g, ""),
                    piva: arr[i].piva,
                    civico: arr[i].civico,
                    indirizzo: arr[i].indirizzo.replace(/[^a-zA-Z ]/g, ""),
                    comune: arr[i].comune,
                    frazione: arr[i].frazione,
                    provincia: arr[i].provincia,
                    stato: arr[i].stato_attivita,
                    cap: arr[i].cap,
                    exporttimestamp: (d.getFullYear() * 10000 + d.getMonth() * 100 + d.getDate() * 1) + '_' + d.getHours() + ':' + d.getMinutes(),
                    ateco: atecoCode,
                    sito: '',
                    tirig: '0'
                }
            )
        }
    }
    return result
}
// GET FETCHED DATA FORMATTED 
async function getDataLinted(fetchStr, ateco) {
    let result;
    try {
        await fetch(fetchStr, {
            headers: {
                Accept: "application/json",
                Authorization: "Bearer " + process.env.OPENAPI_TOKEN
            }
        }).then(response => {
            const contentType = response.headers.get("content-type");
            if (contentType && contentType.indexOf("application/json") !== -1) {
                return response.json().then(dataRes => {
                    let a = getDataFromOpenApi(dataRes.data, ateco)
                    a.map(item => {
                        loadData(item)
                    })
                });
            } else {
                return response.text().then(text => {
                    console.log('The response is a text, probably empty--> ', text, '<--')
                });
            }
        }
        )
    } catch (e) {
        console.log(e)
    }
    return result
}
// LOAD DATA ON POSTGRES TABLE
async function loadData(item) {
    let values = [
        item.lat,
        item.long,
        item.cf,
        item.ragsoc,
        item.piva,
        item.civico,
        item.indirizzo,
        item.comune,
        item.frazione,
        item.provincia,
        item.stato,
        item.cap,
        item.exporttimestamp,
        item.ateco,
        item.sito,
        item.tirig
    ]
    insertRows('sql/insertTable.sql', values)
}
///////////////////////////////////
// START PROGRAM 
///////////////////////////////////
(async () => {
    let ateco = ['2451'];
    let provincia = ['VE']
    let fetchStr = ''
    for (var p = 0; p < provincia.length; p++) {
        for (var a = 0; a < ateco.length; a++) {
            console.log('Loading ..', ateco[a], provincia[p])
            fetchStr = "https://imprese.openapi.it/advance?provincia=" + provincia[p] + "&codice_ateco=" + ateco[a] + "&dipendenti_min=1&dry_run=0";
            //console.log(fetchStr)
            await getDataLinted(fetchStr, ateco[a])
        }
    }
})()
