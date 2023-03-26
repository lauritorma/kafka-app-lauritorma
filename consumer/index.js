import { Kafka } from 'kafkajs';
console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'kafka-checker-servers1'});

// test to see if char is digit

function isDigit(char) {
    return /^\d$/.test(char);
  }

const run = async () => {
    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const producer = kafka.producer();
            await producer.connect();
            if ( message.value.length == 11 && isDigit(message.value.toString().substring(0, 1)) == true ) {
            console.log({
                key:        message.key.toString(),
                partition:  message.partition,
                offset:     message.offset,
                value:      message.value.toString(),
            })
            const success = await producer.send({
                topic: 'checkedresult',
                messages: [
                    {
                        key:        message.key.toString(),
                        partition:  message.partition,
                        offset:     message.offset,
                        value:      message.value.toString(),
                    },
                ],
            });
            if(success) {
                console.log(`Message ${message.key.toString()} successfully from consumer back to the returning stream to producer`)
            } else {
                console.log('Problem writing to stream..');
            }
        
            }
            else if (message.value.length != 11) {
                console.log('length of id:', message.value.toString(), ' is not 11 characters')
            }
            else if (isDigit(message.value.toString().substring(0, 1)) == false) {
                console.log('first character of id:', message.value.toString(), ' is not a digit')
            }
        },
    })

    
}


run().catch(console.error);