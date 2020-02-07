
/**** INIT HARDCODED VARS ***/
const finalDate = new Date('2016-01-01 03:00Z').getTime()
const mongooseCollectionName = 'collectionName'
const targetMongooseCollectionName = mongooseCollectionName
const amountOfLoop = 604800000 // loop week by week up, starting from the current date to the end date ${finalDate}
const mongoConfig = {   database: 'db',
                                    host: 'host',
                                    password: 'password',
                                    port: 'port',
                                    user: 'user' }
const targetMongoConfig = {   database: 'db',
                                    host: 'host',
                                    password: 'password',
                                    port: 'port',
                                    user: 'user' 
                                }
/**** END HARDCODED VARS ***/

const mongoose = require('mongoose')
mongoose.Promise = global.Promise //FIX warning on mongoose verion

const oldConn = mongoose.createConnection(`mongodb://${mongoConfig.user}:${mongoConfig.password}@${mongoConfig.host}:${mongoConfig.port}/${mongoConfig.database}`);
const newConn = mongoose.createConnection(`mongodb://${targetMongoConfig.user}:${targetMongoConfig.password}@${targetMongoConfig.host}:${targetMongoConfig.port}/${targetMongoConfig.database}`);
const startDate = new Date(Date.now() - amountOfLoop);


const startProcess = async date => {
    console.warn('new cicle');
    try {
        let result = await oldConn.collection(mongooseCollectionName).find({ createdAt: { $gte: date } }).toArray()
        console.warn(result.length)
        
        if(result.length > 0)
            await newConn.collection(targetMongooseCollectionName).insertMany(result)   
        
        if(startDate > finalDate)
            executeNewLoop()
        else
            endSuccessProcess()
        
    } catch (error) {
        
        if(error.code !== 11000)  //ERROR 11000 IS DUPLICATED KEY, IGNORE
            endProcess(error)
        else
            executeNewLoop()
        
    }
}

const executeNewLoop = () => {
    console.warn('Execute new loop');
    startDate = new Date(startDate.getTime() - amountOfLoop)
    getNews(startDate);
}

const endProcess = error => {
    console.warn(error)
    oldConn.close()
    newConn.close()
    process.exit(500)
}

const endSuccessProcess = () => {
    console.log( new Date() )
    console.warn('end process')
    oldConn.close()
    newConn.close()
    process.exit(200)
}

//WAIT MONGOOSE CONNECTION SUCCESS AND EXECUTE
setTimeout(function(){
    startProcess(startDate);    
}, 3000)