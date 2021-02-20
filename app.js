const express = require('express')
const { Kafka } = require('kafkajs')
const app = express()
const port = 8080
const kafkaBrokers = process.env.kafkaBrokers || '192.168.0.159:9092'
const kafkaTopics = process.env.kafkaTopics || 'test100k'
//Setting up things
app.use(express.json())

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: kafkaBrokers.split(',')
})
const topics = kafkaTopics.split(',');

getRandomTopic=function (){
    var idx = Math.floor(Math.random() * Math.floor(topics.length))
    return topics[idx];
}
//Setting up handlers

//Actual job
app.post('/', async (req, res) => {
    const producer = kafka.producer()
    try{
        await producer.connect()
        await producer.send({
        topic: getRandomTopic(),
        messages: [
            { value: JSON.stringify(req.body)} 
        ]
        })
        await producer.disconnect()
    }
    catch(e)
    {
        console.log(e);
    }
    res.send('OK')
})
//Just health check
app.get('/', (req, res) =>{
    res.send("Health: OK");
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})