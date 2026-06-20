export interface TicketReceivedMessage {
    ticketId: string;
    customerId: string;
    queryText: string;
    channel: 'email' | 'chat' | 'web';
    submittedAt: string;
  }
  
  export interface TicketAIProcessedMessage {
    ticketId: string;
    customerId: string;
    queryText: string;
    aiReply: string;
    confidenceScore: number; // 0 to 1
    aiModel: string;
    processedAt: string;
  }
  
  export interface TicketAutoResolvedMessage {
    ticketId: string;
    customerId: string;
    aiReply: string;
    resolvedAt: string;
  }
  
  export interface TicketEscalatedMessage {
    ticketId: string;
    customerId: string;
    queryText: string;
    aiReply: string;        // AI's attempt — human can use as a starting point
    confidenceScore: number;
    escalatedAt: string;
  }