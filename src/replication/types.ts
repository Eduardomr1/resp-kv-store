import type net from 'node:net';

/**
 * Replication Role: A server acts as either the primary source of truth ('master')
 * or a secondary follower ('replica').
 */
export type ReplicationRole = 'master' | 'replica';

/**
 * Replication State: Current phase of the replication engine lifecycle.
 */
export type ReplicationState =
  | 'none'
  | 'connecting'
  | 'handshake_ping'
  | 'handshake_replconf_port'
  | 'handshake_replconf_capa'
  | 'handshake_psync'
  | 'online'
  | 'disconnected';

/**
 * Handshake Step: Detailed state machine progress during Master-Replica handshake.
 */
export type HandshakeState =
  | 'NONE'
  | 'PING'
  | 'REPLCONF_PORT'
  | 'REPLCONF_CAPA'
  | 'PSYNC'
  | 'ONLINE';

/**
 * ReplicaConnection: Master-side representation of a connected follower socket.
 */
export interface ReplicaConnection {
  socket: net.Socket;
  state: 'handshake' | 'online' | 'disconnected';
  listeningPort?: number;
  capabilities: Set<string>;
  offset: bigint;
  ackOffset?: bigint;
  lastAckTime?: number;
}

/**
 * Replication Telemetry & Info: Compatible with Redis INFO replication output.
 */
export interface ReplicationInfo {
  role: ReplicationRole;
  master_replid: string;
  master_repl_offset: bigint;
  second_repl_offset?: bigint;
  connected_slaves: number;
  master_host?: string;
  master_port?: number;
  master_link_status?: 'up' | 'down';
}

/**
 * Configuration options for the ReplicationManager.
 */
export interface ReplicationManagerOptions {
  role?: ReplicationRole;
  masterReplId?: string;
  backlogSize?: number;
  listeningPort?: number;
  engine?: any;
  onCommand?: (commandArgs: (string | Buffer)[]) => void;
}
