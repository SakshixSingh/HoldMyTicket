import { Kafka } from 'kafkajs';
import { GoogleGenAI } from '@google/genai';
import {
  TicketReceivedMessage,
  TicketAIProcessedMessage,
} from '../../../shared/type';

const GEMINI_MODEL = 'gemini-2.5-flash';

const kafka = new Kafka({
  clientId: 'ai-worker',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'ai-worker-group' });
const producer = kafka.producer();
const genai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

const SYSTEM_PROMPT = `You are a customer support AI. Given a customer query, respond with
a JSON object with exactly two fields:
- "reply": a helpful, professional response to the customer
- "confidence": a number between 0 and 1

Confidence guide:
- 0.8 to 1.0: Clear answerable questions (tracking, returns, password reset, FAQs)
- 0.5 to 0.7: General questions that may need more context
- 0.0 to 0.4: Account-specific issues, billing disputes, anything needing account access

Respond ONLY with valid JSON. No markdown, no backticks, no explanation.`;

interface AIResponse {
  reply: string;
  confidence: number;
}

async function getAIResponse(queryText: string): Promise<AIResponse> {
  const response = await genai.models.generateContent({
    model: GEMINI_MODEL,
    contents: queryText,
    config: {
      systemInstruction: SYSTEM_PROMPT,
      maxOutputTokens: 1024,
      responseMimeType: 'application/json',
    },
  });

  const text = response.text ?? '';

  try {
    const parsed = JSON.parse(text) as AIResponse;
    return {
      reply: parsed.reply,
      confidence: Math.min(1, Math.max(0, parsed.confidence)),
    };
  } catch {
    return { reply: text, confidence: 0.2 };
  }
}

async function start() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: 'tickets.incoming',
    fromBeginning: false,
  });

  console.log('✓ AI Worker listening on tickets.incoming');

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const ticket = JSON.parse(
        message.value.toString()
      ) as TicketReceivedMessage;

      console.log(`→ Processing ticket ${ticket.ticketId}`);

      try {
        const { reply, confidence } = await getAIResponse(ticket.queryText);

        const processed: TicketAIProcessedMessage = {
          ticketId: ticket.ticketId,
          customerId: ticket.customerId,
          queryText: ticket.queryText,
          aiReply: reply,
          confidenceScore: confidence,
          aiModel: GEMINI_MODEL,
          processedAt: new Date().toISOString(),
        };

        await producer.send({
          topic: 'tickets.ai-processed',
          messages: [{
            key: processed.ticketId,
            value: JSON.stringify(processed),
          }],
        });

        console.log(
          `✓ Ticket ${ticket.ticketId} done — confidence: ${confidence}`
        );
      } catch (err) {
        console.error(`✗ Failed on ticket ${ticket.ticketId}:`, err);
      }
    },
  });
}

const shutdown = async () => {
  console.log('Shutting down AI worker...');
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch(console.error);
