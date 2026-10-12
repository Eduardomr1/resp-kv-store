# RESP Key-Value Storage Engine

Un motor de almacenamiento clave-valor en memoria compatible con el protocolo **RESP** (Redis Serialization Protocol) desarrollado en **TypeScript** sobre sockets TCP nativos, con **cero dependencias externas** (únicamente módulos estándar de Node.js 22).

Diseñado con estándares de **Systems & Backend Core Engineering**: seguridad binaria completa, compresión de memoria $O(1)$, concurrencia asíncrona no bloqueante, múltiples subsistemas de persistencia, replicación maestro-réplica y control optimista de transacciones.

---

## 🏛 Arquitectura del Sistema

```
                      +---------------------------------------+
                      |       Cliente RESP (redis-cli / TCP)  |
                      +-------------------+-------------------+
                                          | Sockets TCP (TCP_NODELAY)
                                          v
                      +---------------------------------------+
                      |         TCP Server & Dispatcher       |
                      +-------------------+-------------------+
                                          |
        +---------------------------------+---------------------------------+
        |                                 |                                 |
        v                                 v                                 v
+----------------+               +-----------------+               +-----------------+
|  RESP Parser   |               |   Transacciones |               |     Pub/Sub     |
| Streaming Ring |               |   MULTI / EXEC  |               |  Channels &     |
| Buffer O(1)    |               |   WATCH (OCC)   |               |  Glob Patterns  |
+----------------+               +-----------------+               +-----------------+
        |                                 |                                 |
        +---------------------------------+---------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                            Polymorphic Storage Engine                             |
|  - String (Buffer)                      - Hash (Map<string, Buffer>)              |
|  - List (Buffer[] + BlockingManager)    - Set (Set<string>)                       |
|  - Sorted Set (Probabilistic SkipList, p=0.25, MaxLevel=16)                       |
+-----------------------------------------+-----------------------------------------+
        |                                 |                                 |
        v                                 v                                 v
+----------------+               +-----------------+               +-----------------+
|   Dual-Layer   |               | Memory Governor |               |   Replicación   |
|   TTL Engine   |               | 16-Candidate    |               | Circular Ring   |
| Passive Lazy + |               | Eviction Pool   |               | Backlog (1MB)   |
| Active 100ms   |               | Morris LFU / LRU|               | PSYNC2 Master / |
| Probabilistic  |               | Volatile-TTL    |               | Replica Stream  |
+----------------+               +-----------------+               +-----------------+
        |                                                           |
        +---------------------------------+-------------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |        Persistencia de Datos          |
                      |  - AOF (Append-Only File + Rewrite)   |
                      |  - RDB (Snapshot Binario REDIS0011)   |
                      +---------------------------------------+
```

---

## ⚡ Principales Características Técnicas

### 1. Zero External Dependencies
- Construido estrictamente con las primitivas nativas de Node.js 22:
  - `node:net` para networking TCP de baja latencia con flags `TCP_NODELAY`.
  - `node:buffer` para manipulación de memoria binaria segura y layouts de bajo nivel.
  - `node:fs` y `node:fs/promises` para flujos I/O atómicos y durabilidad en disco.
  - `node:crypto` para identificadores pseudoaleatorios criptográficos (`replid`).
  - `node:events` y `node:perf_hooks` para señalización reactiva y telemetría de microsegundos.
  - `node:test` y `node:assert` para la suite integral de pruebas.
- Ejecución nativa de TypeScript con Node 22 mediante `--experimental-strip-types` (sin compiladores externos, Webpack o bundlers).

### 2. Parser y Serializador RESP2 Streaming de Alto Rendimiento
- **Cursor de lectura continuo**: Procesa chunks TCP fragmentados o pipelined en un buffer circular compacto amortizado $O(1)$.
- **Zero-Copy & Binary Safety**: Capaz de procesar payloads binarios arbitrarios conteniendo bytes nulos (`\0`) y secuencias CRLF (`\r\n`) en Bulk Strings sin desalinear el protocolo.
- **Buffers preasignados**: Respuestas comunes (`+OK\r\n`, `+PONG\r\n`, `+QUEUED\r\n`, `$-1\r\n`, `*-1\r\n`) se sirven desde buffers inmutables compartidos, reduciendo la presión del Garbage Collector.
- Soporta comandos inline tipo Telnet y comandos estructurados en Arrays RESP.

### 3. Motor Polimórfico de Almacenamiento & Sorted Sets (SkipList)
- Tipos de datos nativos:
  - **Strings**: Buffers binarios puros.
  - **Hashes**: `Map<string, Buffer>` con manipulación de campos a nivel byte.
  - **Lists**: `Buffer[]` con soporte para operaciones en cabecera/cola y colas bloqueantes.
  - **Sets**: `Set<string>` con unicidad estricta y cardinalidad $O(1)$.
  - **Sorted Sets (ZSet)**: Implementación de una estructura de datos probabilística **SkipList** ($p=0.25$, altura máxima 16 niveles) con índices de rango, desempate lexicográfico determinista e índice inverso de miembros en $O(\log N)$.
- Validación estricta de tipos devolviendo errores de protocolo `-WRONGTYPE Operation against a key holding the wrong kind of value`.

### 4. Ciclo de Vida y Dual-Layer TTL
- **Evicción Pasiva (Lazy)**: Al consultar cualquier clave, se verifica su marca de tiempo de expiración contra el reloj del sistema. Si ha expirado, se purga de inmediato y se devuelve `nil`.
- **Barredor Probabilístico Activo**: Cronjob adaptativo (ciclo de 100 ms) inspirado en el algoritmo interno de Redis. Muestrea 20 claves aleatorias del índice secundario denso de expiración. Si más del 25% están vencidas, itera inmediatamente dentro de un presupuesto de tiempo de 5 ms, mitigando la retención de memoria huérfana.
- **Índice Denso Swap-and-Pop**: El mantenimiento del índice secundario de expiración se realiza en un array continuo con técnica de eliminación *swap-and-pop* en $O(1)$, evitando huecos y fragmentación de memoria.

### 5. Memory Governor & Políticas de Evicción
- Control dinámico de memoria con umbral `maxmemory` configurable en tiempo de ejecución (`CONFIG SET maxmemory <bytes>`).
- **Pool de Evicción de 16 Candidatos**: Mantiene los mejores candidatos para ser purgados ordenados según la métrica de política seleccionada.
- Políticas soportadas:
  - `noeviction`: Lanza error `-OOM command not allowed when used memory > 'maxmemory'`.
  - `allkeys-lru` / `volatile-lru`: Evicción basada en tiempo de inactividad (*idle time*).
  - `allkeys-lfu` / `volatile-lfu`: Contador probabilístico logarítmico de Morris (0–255) con decaimiento temporal por minutos transcurridos.
  - `volatile-ttl`: Evicción prioritaria de claves con menor TTL restante.
  - `allkeys-random` / `volatile-random`: Muestreo aleatorio uniforme sin reemplazo.

### 6. Primitivas Bloqueantes Asíncronas (`BLPOP`, `BRPOP`)
- Soporte para colas distribuidas basadas en listas.
- Si la lista contiene elementos, realiza extracción inmediata.
- Si la lista está vacía, suspende el socket del cliente sin bloquear el Event Loop de Node.js mediante un registro de consumidores FIFO.
- Al producirse un `LPUSH` o `RPUSH`, el elemento se entrega directamente al consumidor bloqueado más antiguo (`signalPush`) sin escribir temporalmente en la lista.
- Soporte para timeouts de expiración devolviendo `*-1\r\n` y limpieza automática de registros en desconexión súbita del socket.

### 7. Persistencia Dual: AOF y Snapshots RDB
- **AOF (Append-Only File)**:
  - Registro de transacciones Write-Ahead Log serializado en formato nativo RESP.
  - Políticas de sincronización configurables: `always` (fsync por comando), `everysec` (flush asíncrono cada segundo), y `no` (delegado al OS).
  - Reescritura atómica en segundo plano (`BGREWRITEAOF`) mediante archivo temporal y renombrado atómico para compactación.
  - Replay automático al reiniciar el servidor.
- **RDB (Snapshot Binario `REDIS0011`)**:
  - Volcado binario compacto de todos los tipos de datos con prefijos de longitud, opcodes de tipos (`0x00` a `0x04`) y expiraciones en milisegundos (`0xFC`).
  - Integridad asegurada mediante checksum de 64 bits (CRC64).
  - Operaciones síncronas (`SAVE`) y no bloqueantes en background (`BGSAVE`).

### 8. Replicación Maestro-Réplica (PSYNC v2)
- Replicación continua mediante backlog circular de 1 MB (`master_repl_offset`).
- Negociación de réplicas (`PING` -> `REPLCONF listening-port` -> `REPLCONF capa psync2` -> `PSYNC <replid> <offset>`).
- Soporte para **Sincronización Parcial** (`+CONTINUE`) si la réplica se reconecta dentro de la ventana del backlog, evitando descargas masivas innecesarias.
- Soporte para **Sincronización Completa** (`+FULLRESYNC`) si el offset cayó fuera de la ventana circular.
- Propagación en tiempo real de comandos mutadores a todos los sockets de réplicas conectadas.

### 9. Transacciones y Concurrencia Optimista (OCC)
- Bloques atómicos de ejecución secuencial: `MULTI`, `EXEC`, `DISCARD`.
- Bloqueo optimista mediante `WATCH` y `UNWATCH`: Rastreo de versiones por clave (`keyVersions`). Si una clave observada es modificada por otro cliente antes del `EXEC`, la transacción se aborta de forma segura devolviendo Null Array (`*-1\r\n`).

### 10. Motor de Pub/Sub
- Canales directos (`SUBSCRIBE`, `UNSUBSCRIBE`).
- Canales por patrón glob (`PSUBSCRIBE`, `PUNSUBSCRIBE`).
- Difusión de mensajes (`PUBLISH`) con entrega tanto a suscriptores directos como a patrones coincidentes.
- Aislamiento estricto de contexto: sockets en modo Pub/Sub solo pueden invocar comandos de suscripción o finalización.

### 11. Redis Streams & Append-Only Message Log
- Log inmutable basado en IDs temporales monótonos (`<msTime>-<seqNumber>`).
- Inserción y generación de IDs (`XADD key [MAXLEN count] ID field value ...`) con autogeneración `*` y validación estricta de orden.
- Consultas de rango por límites inclusivos (`XRANGE`, `XREVRANGE`) con comodines `-` y `+` y paginación `COUNT`.
- Medición de tamaño (`XLEN`) y recorte adaptativo (`MAXLEN`).
- Streaming reactivo asíncrono (`XREAD [COUNT count] [BLOCK ms] STREAMS key id`):
  - Lectura de eventos posteriores a un ID (`$`).
  - Suspensión no bloqueante de sockets TCP cuando no hay eventos disponibles, con despacho reactivo inmediato en el instante en que un productor ejecuta `XADD`.

### 12. Escaneo Incremental de Claves & Cursors
- Iteración sin bloqueo del Event Loop:
  - `SCAN cursor [MATCH pattern] [COUNT count]`: Itera sobre el espacio global de claves sin congelar el hilo único de ejecución (a diferencia de `KEYS *`).
  - `HSCAN key cursor [MATCH pattern] [COUNT count]`: Paginación sobre campos y valores de Hashes.
  - `SSCAN key cursor [MATCH pattern] [COUNT count]`: Paginación sobre miembros de Sets.
  - `ZSCAN key cursor [MATCH pattern] [COUNT count]`: Paginación sobre miembros y puntuaciones de Sorted Sets.
- Retorna tuplas `[nextCursor, [elementos]]`, finalizando cuando el cursor retorna a `"0"`.

### 13. Telemetría de Latencia: Slowlog Engine
- Rastreador de consultas lentas en memoria en un buffer circular (`slowlog-max-len`).
- Umbral configurable en microsegundos (`slowlog-log-slower-than`).
- Comandos soportados:
  - `SLOWLOG GET [count]`: Retorna lista estructurada de comandos con ID secuencial, timestamp UNIX, tiempo de ejecución en microsegundos, argumentos del comando y dirección IP/puerto del socket.
  - `SLOWLOG LEN`: Cardinalidad actual del log de consultas lentas.
  - `SLOWLOG RESET`: Limpieza y reinicio del buffer circular.

### 14. Operaciones Bit a Bit & Bitmaps de Alta Densidad
- Direccionamiento de bits Big-Endian sobre cadenas binarias (Strings).
- Expansión dinámica del buffer según el offset sin alterar los datos precedentes.
- Comandos soportados:
  - `SETBIT key offset value`: Modifica el bit en el offset indicado ($0 \le offset \le 2^{32}-1$) y retorna el valor previo (0 o 1).
  - `GETBIT key offset`: Lee el bit en la posición indicada en tiempo $O(1)$.
  - `BITCOUNT key [start end]`: Algoritmo de conteo de bits (popcount) sobre rangos de bytes completos o negativos.
  - `BITPOS key bit [start [end]]`: Búsqueda eficiente del primer bit establecido en 0 o 1.
  - `BITOP AND|OR|XOR|NOT destkey srckey [srckey ...]`: Operaciones booleanas a nivel de bit sobre múltiples buffers con relleno automático de ceros.

### 15. HyperLogLog: Estimación de Cardinalidad Probabilística
- Estructura de datos probabilística para calcular cardinalidades masivas en espacio constante (12 KB por clave, 16,384 registros de 8 bits).
- Función hash Murmur/SHA-256 de 64 bits con extracción de índice de registro (14 bits) y conteo de ceros precedentes ($1 \le \rho \le 50$).
- Media armónica de Flajolet con corrección de sesgo mediante Linear Counting para rangos pequeños ($V > 0$).
- Margen de error teórico típico de $\approx 0.81\%$ ($1.04 / \sqrt{m}$).
- Comandos soportados:
  - `PFADD key element [element ...]`: Añade elementos al bosque de registros del HLL. Retorna 1 si al menos un registro fue actualizado.
  - `PFCOUNT key [key ...]`: Retorna la cardinalidad estimada. Para múltiples claves, realiza una unión en memoria sin alterar las claves fuente.
  - `PFMERGE destkey srckey [srckey ...]`: Fusiona los registros de múltiples fuentes en la clave destino computando el máximo por registro en $O(m)$.

---

## 🚀 Inicio Rápido

### Requisitos
- **Node.js**: v22.0.0 o superior (con soporte nativo para `--experimental-strip-types`).

### Instalación
No se requiere instalar paquetes npm externos. Clona o ubícate en el directorio del proyecto:

```bash
cd scratch/resp-kv-store
```

### Iniciar el Servidor
```bash
npm start
# O directamente:
node --experimental-strip-types src/index.ts
```

Por defecto, el servidor escuchará en `127.0.0.1:6379`. Puedes configurar variables de entorno para puerto y host:
```bash
PORT=6380 HOST=0.0.0.0 npm start
```

### Conexión con `redis-cli`
Puedes interactuar con el motor utilizando el cliente oficial de Redis o herramientas de red:

```bash
redis-cli -p 6379 PING
# Salida: PONG

redis-cli -p 6379 SET user:100 "Alice" EX 60
# Salida: OK

redis-cli -p 6379 GET user:100
# Salida: "Alice"

redis-cli -p 6379 XADD events * sensor "thermostat" temp "21.5"
# Salida: "1710000000000-0"

redis-cli -p 6379 XRANGE events - +
# Salida: 1) 1) "1710000000000-0" 2) 1) "sensor" 2) "thermostat" 3) "temp" 4) "21.5"

redis-cli -p 6379 SCAN 0 MATCH user:* COUNT 10
# Salida: 1) "0" 2) 1) "user:100"

redis-cli -p 6379 SLOWLOG LEN
# Salida: (integer) 0

# Bitmaps
redis-cli -p 6379 SETBIT user:active:flags 10 1
# Salida: (integer) 0
redis-cli -p 6379 GETBIT user:active:flags 10
# Salida: (integer) 1
redis-cli -p 6379 BITCOUNT user:active:flags
# Salida: (integer) 1

# HyperLogLog
redis-cli -p 6379 PFADD unique_visitors "192.168.1.1" "192.168.1.2" "192.168.1.1"
# Salida: (integer) 1
redis-cli -p 6379 PFCOUNT unique_visitors
# Salida: (integer) 2

redis-cli -p 6379 SAVE
redis-cli -p 6379 INFO
```

---

## 🧪 Pruebas Unitarias y de Integración

El proyecto incluye 17 suites de pruebas con **127 pruebas automatizadas** que cubren todas las ramas del sistema:

```bash
npm test
```

### Desglose de Cobertura de Suites:
1. `tests/resp.test.ts`: Parser, serializer, chunking TCP, pipelining, telnet inline, cadenas binarias.
2. `tests/storage.test.ts`: Operaciones CRUD, NX/XX, evicción pasiva y barredor activo TTL.
3. `tests/skiplist.test.ts`: Inserción, desempate lexicográfico, eliminación y slicing de rangos.
4. `tests/polymorphic.test.ts`: Hashes, Lists, Sets y Sorted Sets con validación `-WRONGTYPE`.
5. `tests/aof.test.ts`: Escritura síncrona/asíncrona, fsync y atomic background rewrite (`BGREWRITEAOF`).
6. `tests/transactions.test.ts`: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, abortos CAS por dirty flag y concurrencia.
7. `tests/pubsub.test.ts`: Canales directos, patrones glob, difusión múltiple y limpieza de sockets.
8. `tests/replication.test.ts`: Handshake, circular ring buffer, sincronización parcial vs completa y propagación.
9. `tests/eviction.test.ts`: Pool de 16 candidatos, Morris LFU, temporal decay, LRU y volatile-TTL.
10. `tests/blocking.test.ts`: `BLPOP`/`BRPOP`, entrega FIFO reactiva a clientes suspendidos y expiración de timeouts.
11. `tests/rdb.test.ts`: Codificación binaria, CRC64, persistencia en disco atómica, `SAVE` y `BGSAVE`.
12. `tests/streams.test.ts`: `XADD`, `XRANGE`, `XREVRANGE`, `XLEN`, y `XREAD` con timeout y bloqueo reactivo.
13. `tests/scan.test.ts`: `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` con paginación cursor, `MATCH` y `COUNT`.
14. `tests/slowlog.test.ts`: Buffer circular, filtrado de umbral de microsegundos y comandos `SLOWLOG`.
15. `tests/integration.test.ts`: Ciclo de vida completo end-to-end sobre sockets TCP reales.
16. `tests/bitmap.test.ts`: Direccionamiento de bits Big-Endian, popcount 8-bit, bitpos y operaciones booleanas (`AND`, `OR`, `XOR`, `NOT`).
17. `tests/hll.test.ts`: Estimación probabilística HLL, linear counting, convergencia de error $< 2\%$, unión y fusión de registros.

**Resultado de ejecución**: `127 pass, 0 fail (100% éxito en ~2.4s)`.

---

## 📊 Benchmarks de Rendimiento

Puedes ejecutar el arnés de benchmark nativo incluido en el proyecto:

```bash
npm run bench
```

### Resultados Obtenidos (Entorno de Referencia Node.js 22 sobre Windows Loopback):

| Operación | Volumen Evaluado | Tiempo Total | Rendimiento (Throughput) | Latencia Media |
| :--- | :--- | :--- | :--- | :--- |
| **PING** | 10,000 comandos | 544 ms | **18,383 ops/sec** | ~54 µs / op |
| **GET** | 10,000 comandos | 676 ms | **14,784 ops/sec** | ~67 µs / op |
| **SET** | 10,000 comandos | 1,023 ms | **9,767 ops/sec** | ~102 µs / op |
| **HSET** | 10,000 comandos | 666 ms | **15,011 ops/sec** | ~66 µs / op |
| **PIPELINE (Batch 100)** | 50,000 comandos | 1,422 ms | **35,152 ops/sec** | ~28 µs / op |

---

## 📁 Estructura del Código

```
resp-kv-store/
├── benchmarks/
│   └── bench.ts                 # Arnés de pruebas de rendimiento sobre TCP
├── src/
│   ├── index.ts                 # Punto de entrada principal y servidor CLI
│   ├── server.ts                # Servidor TCP, despacho de comandos y ciclo de vida
│   ├── blocking/                # Subsistema de operaciones bloqueantes (BLPOP, BRPOP)
│   │   ├── manager.ts
│   │   └── types.ts
│   ├── eviction/                # Gobernador de memoria, políticas LRU/LFU y pool de candidatos
│   │   ├── governor.ts
│   │   ├── lfu.ts
│   │   ├── pool.ts
│   │   └── types.ts
│   ├── pubsub/                  # Engine de mensajería Pub/Sub y filtrado de patrones glob
│   │   ├── hub.ts
│   │   └── types.ts
│   ├── replication/             # Replicación Maestro-Réplica, circular backlog y PSYNC2
│   │   ├── backlog.ts
│   │   ├── manager.ts
│   │   └── types.ts
│   ├── resp/                    # Parser streaming y serializador del protocolo RESP2
│   │   ├── parser.ts
│   │   ├── serializer.ts
│   │   └── types.ts
│   ├── storage/                 # Engine en memoria, SkipList, persistencia AOF y RDB
│   │   ├── aof.ts
│   │   ├── bitmap.ts            # Manipulación de bits, popcount y operaciones booleanas
│   │   ├── engine.ts            # Motor de almacenamiento polimórfico y TTL
│   │   ├── hll.ts               # HyperLogLog de 16,384 registros y media armónica
│   │   ├── rdb.ts
│   │   ├── rdb_types.ts
│   │   ├── skiplist.ts
│   │   ├── stream.ts
│   │   └── types.ts
│   ├── telemetry/               # Telemetría de latencia y monitoreo de Slowlog
│   │   └── slowlog.ts
│   └── transactions/            # Engine de transacciones MULTI/EXEC y bloqueo optimista WATCH
│       ├── manager.ts
│       └── types.ts
├── tests/                       # 17 suites de pruebas unitarias y de integración (127 tests)
├── package.json
└── tsconfig.json
```

---

## 📄 Licencia

MIT

