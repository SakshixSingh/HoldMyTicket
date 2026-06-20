import express from 'express';
import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { TicketReceivedMessage } from '../../../shared/type';

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'api-service',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function start() {
  await producer.connect();
  console.log('✓ Producer connected to Kafka');

  app.post('/tickets', async (req, res) => {
    const { customerId, queryText, channel } = req.body;

    if (!customerId || !queryText) {
      return res.status(400).json({ error: 'customerId and queryText required' });
    }

    const message: TicketReceivedMessage = {
      ticketId: uuidv4(),
      customerId,
      queryText,
      channel: channel ?? 'web',
      submittedAt: new Date().toISOString(),
    };

    await producer.send({
      topic: 'tickets.incoming',
      messages: [{
        key: message.ticketId,   // partition by ticketId 
        value: JSON.stringify(message),
      }],
    });

    console.log(`→ Ticket ${message.ticketId} sent to tickets.incoming`);

    return res.status(202).json({
      ticketId: message.ticketId,
      status: 'received',
    });
  });

  app.listen(3000, () => {
    console.log('✓ API listening on http://localhost:3000');
  });
}

start().catch(console.error);