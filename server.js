const net = require('net');

class RedisClient {
	constructor(options = {}) {
		this.host = options.host || '127.0.0.1';
		this.port = options.port || 6379;
		this.connection = null;
		this.isConnected = false;
		this.connectAttempts = 0;
		this.maxConnectAttempts = options.maxConnectAttempts || 3;
		this.connectRetryInterval = options.connectRetryInterval || 1000;
		this.commandQueue = [];
		this.isProcessing = false;
		this.responseBuffer = '';
		this.connectionPool = [];
		this.maxPoolSize = options.maxPoolSize || 10;
		this.newConnectionCount = 0;
	}

	async connect() {
		return new Promise((resolve, reject) => {
			this.connection = new net.Socket();

			const handleConnect = () => {
				this.isConnected = true;
				this.connectAttempts = 0;
				console.log('Connected to Redis server');
				resolve();
			};

			const handleConnectError = (error) => {
				this.isConnected = false;
				this.connectAttempts++;

				console.error('Error connecting to Redis:', error.message);

				if (this.connectAttempts >= this.maxConnectAttempts) {
					reject(new Error('Max connection attempts reached'));
				} else {
					setTimeout(() => {
						this.connect();
					}, this.connectRetryInterval);
				}
			};

			this.connection.once('connect', handleConnect);
			this.connection.once('error', handleConnectError);

			this.connection.connect(this.port, this.host);
		});
	}

	serializeCommand(command, ...args) {
		// Step 1: Serialize each argument and calculate their lengths
		const serializedArgs = args.map((arg) => {
			const argLength = Buffer.byteLength(arg, 'utf-8');
			const serializedArg = `$${argLength}\r\n${arg}`; // Format: $<arg_length>\r\n<arg>
			return serializedArg;
		});

		// Step 2: Calculate the length of the command in bytes
		const commandLength = Buffer.byteLength(command, 'utf-8');
		// Step 3: Combine everything to form the final serialized command
		const serializedCommand = `*${
			args.length + 1
		}\r\n$${commandLength}\r\n${command}\r\n${serializedArgs.join('\r\n')}\r\n`;

		return serializedCommand;
	}

	parseResponse() {
		const endOfResponse = this.responseBuffer.indexOf('\r\n');
		if (endOfResponse === -1) {
			return null;
		}

		const responseLine = this.responseBuffer.slice(0, endOfResponse);
		this.responseBuffer = this.responseBuffer.slice(endOfResponse + 2);

		const responseType = responseLine[0];
		const responseData = responseLine.slice(1);

		switch (responseType) {
			case '+': // Simple Strings
				return responseData;

			case '-': // Errors
				throw new Error(responseData);

			case ':': // Integers
				return parseInt(responseData);

			case '$': // Bulk Strings
				const length = parseInt(responseData);
				if (length === -1) {
					return null; // Null response
				}

				if (this.responseBuffer.length < length + 2) {
					return null; // Not enough data in buffer
				}

				const bulkData = this.responseBuffer.slice(0, length);
				this.responseBuffer = this.responseBuffer.slice(length + 2); // Remove bulk data and trailing \r\n
				return bulkData;

			case '*': // Arrays
				const numElements = parseInt(responseData);
				const array = [];

				for (let i = 0; i < numElements; i++) {
					const element = this.parseResponse();
					array.push(element);
				}

				return array;

			default:
				throw new Error(`Invalid response type: ${responseType}`);
		}
	}

	async sendCommand(command, ...args) {
		return new Promise((resolve, reject) => {
			this.commandQueue.push({ command, args, resolve, reject });

			if (!this.isProcessing) {
				this.processCommands();
			}
		});
	}

	async processCommands() {
		if (this.commandQueue.length === 0) {
			this.isProcessing = false;
			return;
		}

		this.isProcessing = true;

		const { command, args, resolve, reject } = this.commandQueue.shift();
		const commandString = this.serializeCommand(command, ...args);

		this.connection.write(commandString, 'utf-8', () => {
			this.connection.once('data', (data) => {
				this.responseBuffer += data.toString();
				const response = this.parseResponse();

				if (response !== null) {
					resolve(response);
					this.processCommands();
				}
			});

			this.connection.once('error', (err) => {
				reject(err);
			});
		});
	}

	async SET(key, value) {
		return this.sendCommand('SET', key, value);
	}

	async GET(key) {
		return this.sendCommand('GET', key);
	}

	async HSET(hashKey, field, value) {
		return this.sendCommand('HSET', hashKey, field, value);
	}

	async HGET(hashKey, field) {
		return this.sendCommand('HGET', hashKey, field);
	}

	async LPUSH(listKey, ...values) {
		return this.sendCommand('LPUSH', listKey, ...values);
	}

	async LPOP(listKey) {
		return this.sendCommand('LPOP', listKey);
	}

	async ZADD(key, score, member) {
		return this.sendCommand('ZADD', key, score, member);
	}

	async ZRANGE(key, start, stop) {
		return this.sendCommand('ZRANGE', key, start, stop);
	}

	async INCR(key) {
		return this.sendCommand('INCR', key);
	}

	async DECR(key) {
		return this.sendCommand('DECR', key);
	}

	async EXPIRE(key, seconds) {
		return this.sendCommand('EXPIRE', key, seconds);
	}

	async TTL(key) {
		return this.sendCommand('TTL', key);
	}

	async KEYS(pattern) {
		return this.sendCommand('KEYS', pattern);
	}

	async SADD(setKey, ...members) {
		return this.sendCommand('SADD', setKey, ...members);
	}

	async SMEMBERS(setKey) {
		return this.sendCommand('SMEMBERS', setKey);
	}

	async ZSCORE(key, member) {
		return this.sendCommand('ZSCORE', key, member);
	}

	async ZREM(key, member) {
		return this.sendCommand('ZREM', key, member);
	}

	async SCAN(cursor, pattern = '*', count = 10) {
		return this.sendCommand('SCAN', cursor, 'MATCH', pattern, 'COUNT', count);
	}

	async HMSET(hashKey, ...fieldValuePairs) {
		return this.sendCommand('HMSET', hashKey, ...fieldValuePairs);
	}

	async HMGET(hashKey, ...fields) {
		return this.sendCommand('HMGET', hashKey, ...fields);
	}

	async RPUSH(listKey, ...values) {
		return this.sendCommand('RPUSH', listKey, ...values);
	}

	async RPOP(listKey) {
		return this.sendCommand('RPOP', listKey);
	}

	async SINTER(...setKeys) {
		return this.sendCommand('SINTER', ...setKeys);
	}

	async HDEL(hashKey, ...fields) {
		return this.sendCommand('HDEL', hashKey, ...fields);
	}

	async ZCOUNT(key, min, max) {
		return this.sendCommand('ZCOUNT', key, min, max);
	}

	async ZREVRANGE(key, start, stop) {
		return this.sendCommand('ZREVRANGE', key, start, stop);
	}

	async ZRANK(key, member) {
		return this.sendCommand('ZRANK', key, member);
	}

	async PFADD(key, ...elements) {
		return this.sendCommand('PFADD', key, ...elements);
	}

	async PFCOUNT(key) {
		return this.sendCommand('PFCOUNT', key);
	}

	async GEOADD(key, longitude, latitude, member) {
		return this.sendCommand('GEOADD', key, longitude, latitude, member);
	}

	async GEODIST(key, member1, member2, unit = 'm') {
		return this.sendCommand('GEODIST', key, member1, member2, unit);
	}

	async GEOHASH(key, ...members) {
		return this.sendCommand('GEOHASH', key, ...members);
	}

	async HGETALL(hashKey) {
		return this.sendCommand('HGETALL', hashKey);
	}

	async HINCRBY(hashKey, field, increment) {
		return this.sendCommand('HINCRBY', hashKey, field, increment);
	}

	async close() {
		if (this.connection) {
			this.connection.end();
			this.isConnected = false;
			console.log('Connection closed');
		}
	}
}

(async () => {
	const redis = new RedisClient({ host: 'localhost', port: 6379 });

	try {
		await redis.connect();

		// Send SET command
		const setResponse = await redis.SET('name', 'John');
		console.log('SET Response:', setResponse);

		// Send GET command
		const getResponse = await redis.sendCommand('GET', 'name');
		console.log('GET Response:', getResponse);

		// Example usage of additional Redis commands
		await redis.GEOADD('locations', '-73.935242', '40.730610', 'New York');
		await redis.GEOADD('locations', '-118.243683', '34.052235', 'Los Angeles');
		const geohashResponse = await redis.GEOHASH(
			'locations',
			'New York',
			'Los Angeles'
		);
		console.log('GEOHASH Response:', geohashResponse);

		await redis.HSET('user:1', 'age', '30'); // Set an initial age value
		await redis.HINCRBY('user:1', 'age', '2'); // Increment age
		const newAge = await redis.HGET('user:1', 'age');
		console.log('New Age:', newAge);

		const hgetallResponse = await redis.HGETALL('user:1');
		console.log('HGETALL Response:', hgetallResponse);
	} catch (error) {
		console.error('Error:', error);
	}
})();
