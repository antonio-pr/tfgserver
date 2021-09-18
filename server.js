const express = require("express");
const app = express();
var mqtt = require("mqtt");
var client = mqtt.connect("mqtt://broker.hivemq.com");
const sqlite = require('sqlite3').verbose();
var bodyparser = require('body-parser');
app.use(bodyparser.urlencoded({ extended: false}));
app.use(bodyparser.json());

let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
db.run(`CREATE TABLE IF NOT EXISTS CO2DATA(
    ID INTEGER PRIMARY KEY AUTOINCREMENT, 
    VALUE INTEGER, 
    TIMESTAMP INTEGER, 
    ID_SENSOR INTEGER
    )`);

db.run(`CREATE TABLE IF NOT EXISTS SENSORNAME(
    ID INTEGER PRIMARY KEY AUTOINCREMENT, 
    MAC_ADDRESS TEXT UNIQUE, 
    NAME TEXT,
    THRESHOLD INTEGER,
    N_PEOPLE INTEGER, 
    VOLUME INTEGER
    )`);

db.close();

client.on('connect', () => {
    console.log("Connected to broker...")
    client.subscribe("/tfg/sensornode/data/co2/+")
});

client.on('message', (topic, message) => {
    console.log(topic, message.toString());
    let regex = /([0-9A-Fa-f]{2}:){5}([0-9A-Fa-f]{2})/g;
    let mac_adress = topic.match(regex);
    let sensor_data = JSON.parse(message.toString());

    //Insert new c02 data into database
    if(mac_adress && sensor_data["co2"] && sensor_data["co2"] > 390 && sensor_data["co2"] < 3000) {
        let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE);
        let query = "SELECT * FROM SENSORNAME";

        console.log("sí");
        //Query to get the sensors saved in the database
        db.all(query, (err,row) => {
            if(err) {
                console.log(err)
                return;
            }else {
                if(row.length) {
                    console.log(row);
                    var pos = null;
                    //Search the incoming MAC address in the database
                    for(var i=0; i<row.length;i++) {
                        if(mac_adress[0] == row[i].MAC_ADDRESS) {
                            console.log(row[i].MAC_ADDRESS)
                            pos = i;
                        }
                    }
                    let now = new Date();
                    //If the incoming sensor is in the database
                    if(pos!=null) {
                        let insertData = db.prepare('INSERT INTO CO2DATA (VALUE, TIMESTAMP, ID_SENSOR) VALUES(?,?,?)');
                        
                        //insertData.run(sensor_data["co2"], now.toLocaleString(), row[pos].ID);
                        insertData.run(sensor_data["co2"], now, row[pos].ID);
                        insertData.finalize();
                    }else {//If it is a new sensor
                        let insertSensor = db.prepare('INSERT INTO SENSORNAME (MAC_ADDRESS,NAME,THRESHOLD) VALUES(?,?,?)');
                        insertSensor.run(mac_adress[0], "sensor", 800);
                        insertSensor.finalize();

                        let insertData = db.prepare('INSERT INTO CO2DATA (VALUE, TIMESTAMP, ID_SENSOR) VALUES(?,?,?)');
                        //insertData.run(sensor_data["co2"], now.toLocaleString(), row[row.length-1].ID+1);
                        insertData.run(sensor_data["co2"], now, row[row.length-1].ID+1);
                        insertData.finalize();
                    }   
                }else {
                    let now = new Date();
                    let insertSensor = db.prepare('INSERT INTO SENSORNAME (MAC_ADDRESS,NAME,THRESHOLD) VALUES(?,?,?)');
                    insertSensor.run(mac_adress[0], "sensor",800);
                    insertSensor.finalize();

                    let insertData = db.prepare('INSERT INTO CO2DATA (VALUE, TIMESTAMP, ID_SENSOR) VALUES(?,?,?)');
                    //insertData.run(sensor_data["co2"], now.toLocaleString(), 1);
                    insertData.run(sensor_data["co2"], now, 1);
                    insertData.finalize();
                }
            }
        })
        db.close();
    }
});

app.get('/', async (req, res) => {
    res.send('ok');
  });
  
const fillObject = (id_mac,id_name,data,time,threshold,n_people,volume) => {
    return({
        id: [id_mac,id_name],
        data: data,
        time: time,
        threshold: threshold,
        n_people: n_people,
        volume: volume
    })
}

//Post request to init the chart in the client side. It returns the last N measures of the available sensors to the frontend.
app.post("/initchart", async (req, res) => {
    let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
    let queryNames = "SELECT * FROM SENSORNAME";
    let queryValues = "SELECT VALUE, TIMESTAMP FROM CO2DATA WHERE ID_SENSOR = ? ORDER BY ID DESC LIMIT ?"; // ORDER BY ID DESC LIMIT ?";

    //Query to get names 
    db.all(queryNames, (err,row) => {
        if(err) {
            console.log(err)
            return;
        }else {
            console.log(row);
            let res_object = [];
            let id_array = []
            row.forEach(element => {
                let sensor_aux;
                if(element.NAME == "sensor")
                {
                    sensor_aux = element.NAME + element.ID.toString()
                }
                else
                {
                    sensor_aux = element.NAME
                }
                
                res_object.push(fillObject(element.MAC_ADDRESS, sensor_aux, null, null, element.THRESHOLD, element.N_PEOPLE, element.VOLUME));
                id_array.push(element.ID)
            });
            
            //Query to get the data 
            let values_aux = [];
            let time_aux = []
            let data = [];
            let timestamp = [];
            const saveDatos = () => {
                return new Promise((resolve, reject) => {
                    id_array.forEach((element) => {
                        db.all(queryValues, [element, req.body.lastmeasures], (err,rowV) => {
                            if(err) {
                                console.log(err);
                                return;
                            }else {
                                //console.log("row values" + element.toString(), rowV);
                                rowV.forEach(co2Value => {
                                    values_aux.unshift(co2Value.VALUE);
                                    const date_aux = new Date(co2Value.TIMESTAMP)
                                    time_aux.unshift(date_aux.toLocaleString())
                                })
                            }
                            data.push(values_aux);
                            timestamp.push(time_aux);
                            values_aux = [];
                            time_aux = []
                        })
                    });
                    setTimeout(() => {
                        resolve([data,timestamp]);
                    },750);
                });
            }

            async function fetchingData() {
                try{
                    [resultData, resultTime] = await saveDatos();
                    console.log(resultTime);
                    resultData.forEach((element,index) => {
                        res_object[index].data = element;
                        res_object[index].time = resultTime[index]
                    })
                    console.log("res_object",res_object)
                    res.send(res_object)
                } catch (err) {
                    console.log(err)
                }
            }
            fetchingData();
        }
    });
});

//Post request to init the chart in the client. It returns the measures in between two dates.
app.post("/initchartdate", async (req, res) => {
    let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
    let queryNames = "SELECT * FROM SENSORNAME";
    let queryValues = "SELECT VALUE, TIMESTAMP FROM CO2DATA WHERE ID_SENSOR = ? AND TIMESTAMP >= ? AND TIMESTAMP <= ? ORDER BY ID"; // ORDER BY ID DESC LIMIT ?";
    console.log("date1:", req.body.date1)
    console.log("date2:", req.body.date2)

    //Query to get mac address and name of sensors
    db.all(queryNames, (err,row) => {
        if(err) {
            console.log(err)
            return;
        }else {
            console.log(row);
            let res_object = [];
            let id_array = []
            row.forEach(element => {
                res_object.push(fillObject(element.MAC_ADDRESS, element.NAME + element.ID.toString(), null, null, element.THRESHOLD, element.N_PEOPLE, element.VOLUME));
                id_array.push(element.ID)
            });
            
            let values_aux = [];
            let time_aux = []
            let data = [];
            let timestamp = [];

            //Function with a query to get the data from the sensors
            const saveDatos = () => {
                return new Promise((resolve, reject) => {
                    id_array.forEach((element) => {
                        db.all(queryValues, [element, req.body.date1, req.body.date2], (err,rowV) => {
                            if(err) {
                                console.log(err);
                                return;
                            }else {
                                //console.log("row values" + element.toString(), rowV);
                                rowV.forEach(co2Value => {
                                    values_aux.push(co2Value.VALUE);
                                    const date_aux = new Date(co2Value.TIMESTAMP)
                                    time_aux.push(date_aux.toLocaleString())
                                })
                            }
                            data.push(values_aux);
                            timestamp.push(time_aux);
                            values_aux = [];
                            time_aux = []
                        })
                    });
                    setTimeout(() => {
                        resolve([data,timestamp]);
                    },750);
                });
            }

            async function fetchingData() {
                try{
                    [resultData, resultTime] = await saveDatos();
                    console.log(resultTime);
                    resultData.forEach((element,index) => {
                        res_object[index].data = element;
                        res_object[index].time = resultTime[index]
                    })
                    console.log("res_object",res_object)
                    res.send(res_object)
                } catch (err) {
                    console.log(err)
                }
            }
            fetchingData();
        }
    });
});

//Post petition to change the name of the sensor in the app
app.put("/changename", async (req, res) => {
    console.log(req.body);
    let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
    let sql = `UPDATE SENSORNAME
            SET NAME = ?
            WHERE MAC_ADDRESS = ?`;
    
    let data = [req.body.newName, req.body.id_sensor];

    db.run(sql, data, function(err) {
        if (err) {
            return console.error(err.message);
        }
        console.log(`Row(s) updated: ${this.changes}`);
    });
});

//Post petition to change the name of the sensor in the app
app.put("/changethreshold", async (req, res) => {
    console.log(req.body);
    let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
    let sql = `UPDATE SENSORNAME
            SET THRESHOLD = ?
            WHERE MAC_ADDRESS = ?`;
    
    let data = [req.body.newThreshold, req.body.id_sensor];

    db.run(sql, data, function(err) {
        if (err) {
            return console.error(err.message);
        }
        console.log(`Row(s) updated: ${this.changes}`);
        res.send("ok")
    });
});

//Post petition to change the name of the sensor in the app
app.put("/changesteadystate", async (req, res) => {
    console.log(req.body);
    let db = new sqlite.Database('test.db', sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
    let sql = `UPDATE SENSORNAME
            SET N_PEOPLE = ?, VOLUME = ?
            WHERE MAC_ADDRESS = ?`;
    
    let data = [req.body.n_people, req.body.volume, req.body.id_sensor]

    db.run(sql, data, function(err) {
        if (err) {
            return console.error(err.message);
        }
        console.log(`Row(s) updated: ${this.changes}`);
        res.send("ok")
    });
});

app.listen(1337, () => {
    console.log("Server running on port 1337")
}); 