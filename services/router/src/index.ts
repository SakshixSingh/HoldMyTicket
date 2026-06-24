import { Kafka } from 'kafkajs';
import {
  TicketAIProcessedMessage,
  TicketAutoResolvedMessage,
  TicketEscalatedMessage,
} from '../../../shared/type';

const CONFIDENCE_THRESHOLD = 0.7; // single number to tune, not buried in logic

const kafka = new Kafka({
  clientId: 'router',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'router-group' });
const producer = kafka.producer();

async function start() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ 
    topic: 'tickets.ai-processed', 
    fromBeginning: false 
  });

  console.log('✓ Router listening on tickets.ai-processed');

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const ticket = JSON.parse(
        message.value.toString()
      ) as TicketAIProcessedMessage;

      if (ticket.confidenceScore >= CONFIDENCE_THRESHOLD) {

        const autoResolved: TicketAutoResolvedMessage = {
          ticketId: ticket.ticketId,
          customerId: ticket.customerId,
          aiReply: ticket.aiReply,
          resolvedAt: new Date().toISOString(),
        };

        await producer.send({
          topic: 'tickets.auto-resolved',
          messages: [{ 
            key: ticket.ticketId, 
            value: JSON.stringify(autoResolved) 
          }],
        });

        console.log(
          `✓ ${ticket.ticketId} → auto-resolved (${ticket.confidenceScore})`
        );

      } else {

        const escalated: TicketEscalatedMessage = {
          ticketId: ticket.ticketId,
          customerId: ticket.customerId,
          queryText: ticket.queryText,
          aiReply: ticket.aiReply,
          confidenceScore: ticket.confidenceScore,
          escalatedAt: new Date().toISOString(),
        };

        await producer.send({
          topic: 'tickets.escalated',
          messages: [{ 
            key: ticket.ticketId, 
            value: JSON.stringify(escalated) 
          }],
        });

        console.log(
          `⚠ ${ticket.ticketId} → escalated (${ticket.confidenceScore})`
        );
      }
    },
  });
}

const shutdown = async () => {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch(console.error);