import { ethers } from 'ethers';
import axios from 'axios';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();

// Cache configuration
const CACHE_DIR = path.join(process.cwd(), 'cache');
const USE_CACHE = process.env.USE_CACHE === 'true'; // Set USE_CACHE=true to load from cache

// IMPORTANT: These block ranges only capture events within this range.
// If positions were created BEFORE FROM_BLOCK, you won't see their initial
// liquidity additions, which may cause decrease > increase imbalances.
// To capture complete position history, set FROM_BLOCK to the pool creation block.
const FROM_BLOCK = 38913515;
const TO_BLOCK = 40249153;

const HYPERSYNC_API_KEY = process.env.HYPERSYNC_API_KEY;
// Using chain-id 1 for Ethereum mainnet (adjust if needed)
const CHAIN_ID = 8453;
const HYPERSYNC_URL = `https://${CHAIN_ID}.hypersync.xyz/query`;

const CL_POOL = "0xff948480D67f83F6F0D2b87217ffC662f19F4B47";
// Based on the images, Transfer events are from the NFT contract
const NFT_CONTRACT = "0xa990c6a764b73bf43cee5bb40339c3322fb9d55f";

// Mint event signature: Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
const MINT_EVENT_SIGNATURE = ethers.id("Mint(address,address,int24,int24,uint128,uint256,uint256)");
// Burn event signature: Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
const BURN_EVENT_SIGNATURE = ethers.id("Burn(address,int24,int24,uint128,uint256,uint256)");
// Collect event signature: Collect(address indexed owner, address recipient, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount0, uint128 amount1)
const COLLECT_EVENT_SIGNATURE = ethers.id("Collect(address,address,int24,int24,uint128,uint128)");
// Transfer event signature: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
const TRANSFER_EVENT_SIGNATURE = ethers.id("Transfer(address,address,uint256)");
// IncreaseLiquidity event signature: IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
const INCREASE_LIQUIDITY_EVENT_SIGNATURE = ethers.id("IncreaseLiquidity(uint256,uint128,uint256,uint256)");
// DecreaseLiquidity event signature: DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
const DECREASE_LIQUIDITY_EVENT_SIGNATURE = ethers.id("DecreaseLiquidity(uint256,uint128,uint256,uint256)");

console.log('TRANSFER_EVENT_SIGNATURE : ', TRANSFER_EVENT_SIGNATURE);

interface MintEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	sender: string;
	owner: string;
	tickLower: bigint;
	tickUpper: bigint;
	amount: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface BurnEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	owner: string;
	tickLower: bigint;
	tickUpper: bigint;
	amount: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface CollectEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	owner: string;
	recipient: string;
	tickLower: bigint;
	tickUpper: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface TransferEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	from: string;
	to: string;
	tokenId: bigint;
	timestamp: number | null;
}

interface MatchedMintEvent {
	mint: MintEvent;
	transfer: TransferEvent;
}

interface MatchedCollectEvent {
	collect: CollectEvent;
	transfer: TransferEvent;
}

interface IncreaseLiquidityEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	tokenId: bigint;
	liquidity: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

interface DecreaseLiquidityEvent {
	blockNumber: number;
	transactionHash: string;
	logIndex: number;
	tokenId: bigint;
	liquidity: bigint;
	amount0: bigint;
	amount1: bigint;
	timestamp: number | null;
}

// Streaming version that processes logs without storing them all in memory
async function fetchAndProcessLogs(
	client: string,
	contractAddress: string,
	eventTopic: string,
	startBlock: number,
	endBlock: number,
	processCallback: (log: any) => void
): Promise<number> {
	console.log(`[fetchAndProcessLogs] Starting fetch for contract ${contractAddress} from block ${startBlock} to ${endBlock}`);
	let currentBlock = startBlock;
	let totalProcessed = 0;

	const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
	const isRetryableError = (error: any): boolean => {
		const retryableCodes = ['EPIPE', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNREFUSED'];
		const retryableStatusCodes = [429, 500, 502, 503, 504];
		
		if (error.code && retryableCodes.includes(error.code)) {
			return true;
		}
		if (error.response && retryableStatusCodes.includes(error.response.status)) {
			return true;
		}
		return false;
	};

	while (currentBlock <= endBlock) {
		let retries = 0;
		const maxRetries = 5;
		let success = false;

		while (retries < maxRetries && !success) {
			try {
				console.log(`[fetchLogs] Fetching logs from block ${currentBlock}...${retries > 0 ? ` (retry ${retries}/${maxRetries})` : ''}`);
				const queryResponse = await axios.post(client, {
					from_block: currentBlock,
					to_block: endBlock,
					logs: [
						{
							address: [contractAddress],
							topics: [[eventTopic]]
						}
					],
					field_selection: {
						log: [
							'block_number',
							'log_index',
							'transaction_index',
							'transaction_hash',
							'data',
							'address',
							'topic0',
							'topic1',
							'topic2',
							'topic3'
						],
						block: ['number', 'timestamp']
					}
				},
	            {
	                headers: {
	                  Authorization: `Bearer ${HYPERSYNC_API_KEY}`,
	                },
	              });

				const nextBlock = queryResponse.data.next_block;
				const dataArray = queryResponse.data.data || [];
				
				console.log(`[fetchLogs] API response: next_block=${nextBlock}, data length=${dataArray.length}`);

				// Find the highest block number in this batch and process logs incrementally
				let maxBlockInBatch = currentBlock - 1;
				if (dataArray.length > 0) {
					// Process each entry immediately to avoid memory buildup
					let logCount = 0;
					for (const entry of dataArray) {
						if (!entry || !entry.logs || !Array.isArray(entry.logs)) {
							continue;
						}
						
						// Create a map of block_number to timestamp for this entry
						const blockMap = new Map(
							(entry.blocks || []).map((block: any) => [block.number, parseInt(block.timestamp, 16)])
						);

						// Process logs from this entry immediately
						for (const log of entry.logs) {
							if (log.block_number && log.block_number > maxBlockInBatch) {
								maxBlockInBatch = log.block_number;
							}
							// Process log immediately via callback instead of storing
							const processedLog = {
								...log,
								timestamp: blockMap.get(log.block_number) || null
							};
							processCallback(processedLog);
							logCount++;
							totalProcessed++;
						}
					}
					console.log(`[fetchLogs] Received ${dataArray.length} response entries with ${logCount} total logs in this batch, max block: ${maxBlockInBatch}`);
				}

				// Check if we should continue
				if (!nextBlock) {
					console.log(`[fetchLogs] No next_block returned, stopping fetch`);
					break;
				}
				
				// If next_block is beyond end block, we're done
				if (nextBlock > endBlock) {
					console.log(`[fetchLogs] next_block ${nextBlock} exceeds endBlock ${endBlock}, stopping fetch`);
					break;
				}
				
				// If next_block is less than currentBlock, something is wrong - increment to avoid infinite loop
				if (nextBlock < currentBlock) {
					console.warn(`[fetchLogs] next_block ${nextBlock} is less than currentBlock ${currentBlock}, incrementing currentBlock`);
					currentBlock = currentBlock + 1;
				}
				// If next_block equals currentBlock and we got no data, we're done
				else if (nextBlock === currentBlock && dataArray.length === 0) {
					console.log(`[fetchLogs] next_block equals currentBlock and no data returned, stopping fetch`);
					break;
				}
				// If next_block equals currentBlock but we got data, increment by 1 to avoid infinite loop
				else if (nextBlock === currentBlock) {
					console.log(`[fetchLogs] next_block equals currentBlock but we got data, incrementing to ${currentBlock + 1}`);
					currentBlock = currentBlock + 1;
				}
				// If next_block equals endBlock
				else if (nextBlock === endBlock) {
					// If we got no data, we're done
					if (dataArray.length === 0) {
						console.log(`[fetchLogs] next_block equals endBlock and no data returned, stopping fetch`);
						break;
					}
					// If we got data but maxBlockInBatch is less than endBlock, there might be more data
					else if (maxBlockInBatch < endBlock) {
						console.log(`[fetchLogs] next_block equals endBlock but maxBlockInBatch ${maxBlockInBatch} < endBlock ${endBlock}, continuing from ${maxBlockInBatch + 1}`);
						currentBlock = maxBlockInBatch + 1;
					}
					// If we got data and maxBlockInBatch equals endBlock, we're done
					else {
						console.log(`[fetchLogs] next_block equals endBlock and maxBlockInBatch ${maxBlockInBatch} equals endBlock, stopping fetch`);
						break;
					}
				} else {
					currentBlock = nextBlock;
				}
				
				console.log(`[fetchAndProcessLogs] Next block: ${currentBlock}, Total processed logs so far: ${totalProcessed}`);
				success = true;
			} catch (error: any) {
				if (isRetryableError(error) && retries < maxRetries - 1) {
					retries++;
					const delay = Math.min(1000 * Math.pow(2, retries - 1), 10000); // Exponential backoff, max 10s
					console.log(`[fetchAndProcessLogs] Retryable error (${error.code || error.response?.status}) at block ${currentBlock}, retrying in ${delay}ms...`);
					await sleep(delay);
				} else {
					console.error(`[fetchAndProcessLogs] Error fetching logs at block ${currentBlock} (attempt ${retries + 1}/${maxRetries}):`, error.code || error.message);
					// Log more details about the error
					if (error.response) {
						console.error(`[fetchAndProcessLogs] Response status: ${error.response.status}`);
						console.error(`[fetchAndProcessLogs] Response data:`, JSON.stringify(error.response.data, null, 2));
					}
					if (retries >= maxRetries - 1) {
						console.error(`[fetchAndProcessLogs] Max retries reached. Stopping fetch. Total logs collected so far: ${totalProcessed}`);
						break;
					}
					retries++;
					const delay = Math.min(1000 * Math.pow(2, retries - 1), 10000);
					await sleep(delay);
				}
			}
		}

		if (!success) {
			console.error(`[fetchAndProcessLogs] Failed to fetch after ${maxRetries} attempts. Breaking loop.`);
			break;
		}
	}

	console.log(`[fetchAndProcessLogs] Completed. Total processed logs: ${totalProcessed}`);
	return totalProcessed;
}

function decodeMintEvent(log: any): MintEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeMintEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			sender: decoded.args.sender.toLowerCase(),
			owner: decoded.args.owner.toLowerCase(),
			tickLower: decoded.args.tickLower,
			tickUpper: decoded.args.tickUpper,
			amount: decoded.args.amount,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeMintEvent] Successfully decoded Mint event: tx=${decodedEvent.transactionHash}, sender=${decodedEvent.sender}, owner=${decodedEvent.owner}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeMintEvent] Error decoding Mint event:", error);
		return null;
	}
}

function decodeBurnEvent(log: any): BurnEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
		]);

		// Construct topics array (topic0 is event signature, topic1 is indexed owner, topic2 is tickLower, topic3 is tickUpper)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);

		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeBurnEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			owner: decoded.args.owner.toLowerCase(),
			tickLower: decoded.args.tickLower,
			tickUpper: decoded.args.tickUpper,
			amount: decoded.args.amount,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeBurnEvent] Successfully decoded Burn event: tx=${decodedEvent.transactionHash}, owner=${decodedEvent.owner}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeBurnEvent] Error decoding Burn event:", error);
		return null;
	}
}

function decodeCollectEvent(log: any): CollectEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Collect(address indexed owner, address recipient, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount0, uint128 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1, topic2, topic3 are indexed)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeCollectEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			owner: decoded.args.owner.toLowerCase(),
			recipient: decoded.args.recipient.toLowerCase(),
			tickLower: decoded.args.tickLower,
			tickUpper: decoded.args.tickUpper,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeCollectEvent] Successfully decoded Collect event: tx=${decodedEvent.transactionHash}, owner=${decodedEvent.owner}, recipient=${decodedEvent.recipient}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeCollectEvent] Error decoding Collect event:", error);
		return null;
	}
}

function decodeTransferEvent(log: any): TransferEvent | null {
	try {
		const iface = new ethers.Interface([
			"event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
		]);
		
		// Construct topics array (all topics are indexed for Transfer)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeTransferEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			from: decoded.args.from.toLowerCase(),
			to: decoded.args.to.toLowerCase(),
			tokenId: decoded.args.tokenId,
			timestamp: log.timestamp,
		};
		console.log(`[decodeTransferEvent] Successfully decoded Transfer event: tx=${decodedEvent.transactionHash}, from=${decodedEvent.from}, to=${decodedEvent.to}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeTransferEvent] Error decoding Transfer event:", error);
		return null;
	}
}

function decodeIncreaseLiquidityEvent(log: any): IncreaseLiquidityEvent | null {
	try {
		const iface = new ethers.Interface([
			"event IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed tokenId)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeIncreaseLiquidityEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			tokenId: decoded.args.tokenId,
			liquidity: decoded.args.liquidity,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeIncreaseLiquidityEvent] Successfully decoded IncreaseLiquidity event: tx=${decodedEvent.transactionHash}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeIncreaseLiquidityEvent] Error decoding IncreaseLiquidity event:", error);
		return null;
	}
}

function decodeDecreaseLiquidityEvent(log: any): DecreaseLiquidityEvent | null {
	try {
		const iface = new ethers.Interface([
			"event DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)",
		]);
		
		// Construct topics array (topic0 is the event signature, topic1 is indexed tokenId)
		const topics = [log.topic0, log.topic1, log.topic2, log.topic3].filter(Boolean);
		
		const decoded = iface.parseLog({
			topics: topics,
			data: log.data,
		});

		if (!decoded) {
			console.log(`[decodeDecreaseLiquidityEvent] Failed to decode log at block ${log.block_number}, log_index ${log.log_index}`);
			return null;
		}

		const decodedEvent = {
			blockNumber: log.block_number,
			transactionHash: log.transaction_hash,
			logIndex: log.log_index,
			tokenId: decoded.args.tokenId,
			liquidity: decoded.args.liquidity,
			amount0: decoded.args.amount0,
			amount1: decoded.args.amount1,
			timestamp: log.timestamp,
		};
		console.log(`[decodeDecreaseLiquidityEvent] Successfully decoded DecreaseLiquidity event: tx=${decodedEvent.transactionHash}, tokenId=${decodedEvent.tokenId.toString()}`);
		return decodedEvent;
	} catch (error) {
		console.error("[decodeDecreaseLiquidityEvent] Error decoding DecreaseLiquidity event:", error);
		return null;
	}
}

// Simple CSV escaping - wrap in quotes if contains comma, quote, or newline
function escapeCsvValue(value: string): string {
	if (value.includes(',') || value.includes('"') || value.includes('\n')) {
		return '"' + value.replace(/"/g, '""') + '"';
	}
	return value;
}

// Cache helper functions
function ensureCacheDir(): void {
	if (!fs.existsSync(CACHE_DIR)) {
		fs.mkdirSync(CACHE_DIR, { recursive: true });
		console.log(`[cache] Created cache directory: ${CACHE_DIR}`);
	}
}

function getCacheFilePath(eventType: string): string {
	return path.join(CACHE_DIR, `${eventType}_${CL_POOL}.json`);
}

function saveToCache<T>(eventType: string, data: T[]): void {
	ensureCacheDir();
	const filePath = getCacheFilePath(eventType);
	// Use NDJSON format (one JSON object per line) to avoid string length limits
	const writeStream = fs.createWriteStream(filePath);
	for (const item of data) {
		const line = JSON.stringify(item, (key, value) =>
			typeof value === 'bigint' ? value.toString() : value
		);
		writeStream.write(line + '\n');
	}
	writeStream.end();
	console.log(`[cache] Saved ${data.length} ${eventType} events to ${filePath}`);
}

async function loadFromCache<T>(eventType: string): Promise<T[] | null> {
	const filePath = getCacheFilePath(eventType);
	if (!fs.existsSync(filePath)) {
		console.log(`[cache] No cache file found for ${eventType}`);
		return null;
	}
	// Read NDJSON format using streaming to avoid memory limits
	const readline = await import('readline');
	const fileStream = fs.createReadStream(filePath);
	const rl = readline.createInterface({
		input: fileStream,
		crlfDelay: Infinity
	});

	const data: T[] = [];
	for await (const line of rl) {
		if (line.trim() !== '') {
			data.push(JSON.parse(line));
		}
	}
	console.log(`[cache] Loaded ${data.length} ${eventType} events from cache`);
	return data;
}

// Convert cached data back to proper types (BigInt fields)
function restoreBigInts<T>(obj: any, bigIntFields: string[]): T {
	const result = { ...obj };
	for (const field of bigIntFields) {
		if (result[field] !== undefined && result[field] !== null) {
			result[field] = BigInt(result[field]);
		}
	}
	return result as T;
}

async function main() {
	console.log("[main] Starting main function");

	// Check if API key is set
	if (!HYPERSYNC_API_KEY) {
		console.error("[main] ERROR: HYPERSYNC_API_KEY is not set in .env file!");
		console.error("[main] Please create a .env file with: HYPERSYNC_API_KEY=your_api_key_here");
		process.exit(1);
	}

	console.log(`[main] Configuration: FROM_BLOCK=${FROM_BLOCK}, TO_BLOCK=${TO_BLOCK}`);
	console.log(`[main] CL_POOL=${CL_POOL}, NFT_CONTRACT=${NFT_CONTRACT}`);
	console.log(`[main] API Key present: ${HYPERSYNC_API_KEY.substring(0, 10)}...`);
	
	const outputPath = path.join(process.cwd(), `events_${CL_POOL}.csv`);
	const outputFile = fs.createWriteStream(outputPath);
	
	// Handle file write errors
	outputFile.on('error', (err) => {
		console.error('[main] File write error:', err);
		process.exit(1);
	});
	
	// Write CSV header
	const headers = ['type', 'transactionHash', 'blockNumber', 'amount', 'amount0', 'amount1', 'user'];
	outputFile.write(headers.map(escapeCsvValue).join(',') + '\n');
	
	let totalMintEvents = 0;
	let totalBurnEvents = 0;
	let totalCollectEvents = 0;
	let totalIncreaseLiquidityEvents = 0;
	let totalDecreaseLiquidityEvents = 0;
	let matchedIncreaseLiquidityCount = 0;
	let matchedDecreaseLiquidityCount = 0;

	// ==================== DATA LOADING PHASE ====================
	// Either fetch from HyperSync API or load from cache

	let allTransferEvents: TransferEvent[] = [];
	let allMintEvents: MintEvent[] = [];
	let allBurnEvents: BurnEvent[] = [];
	let allCollectEvents: CollectEvent[] = [];
	let allIncreaseLiquidityEvents: IncreaseLiquidityEvent[] = [];
	let allDecreaseLiquidityEvents: DecreaseLiquidityEvent[] = [];

	if (USE_CACHE) {
		console.log("[main] USE_CACHE=true, loading from cache...");

		// Load Transfer events
		const cachedTransfers = await loadFromCache<any>('transfer');
		if (cachedTransfers) {
			allTransferEvents = cachedTransfers.map((e: any) => restoreBigInts<TransferEvent>(e, ['tokenId']));
		}

		// Load Mint events
		const cachedMints = await loadFromCache<any>('mint');
		if (cachedMints) {
			allMintEvents = cachedMints.map((e: any) => restoreBigInts<MintEvent>(e, ['tickLower', 'tickUpper', 'amount', 'amount0', 'amount1']));
		}

		// Load Burn events
		const cachedBurns = await loadFromCache<any>('burn');
		if (cachedBurns) {
			allBurnEvents = cachedBurns.map((e: any) => restoreBigInts<BurnEvent>(e, ['tickLower', 'tickUpper', 'amount', 'amount0', 'amount1']));
		}

		// Load Collect events
		const cachedCollects = await loadFromCache<any>('collect');
		if (cachedCollects) {
			allCollectEvents = cachedCollects.map((e: any) => restoreBigInts<CollectEvent>(e, ['tickLower', 'tickUpper', 'amount0', 'amount1']));
		}

		// Load IncreaseLiquidity events
		const cachedIncreases = await loadFromCache<any>('increaseLiquidity');
		if (cachedIncreases) {
			allIncreaseLiquidityEvents = cachedIncreases.map((e: any) => restoreBigInts<IncreaseLiquidityEvent>(e, ['tokenId', 'liquidity', 'amount0', 'amount1']));
		}

		// Load DecreaseLiquidity events
		const cachedDecreases = await loadFromCache<any>('decreaseLiquidity');
		if (cachedDecreases) {
			allDecreaseLiquidityEvents = cachedDecreases.map((e: any) => restoreBigInts<DecreaseLiquidityEvent>(e, ['tokenId', 'liquidity', 'amount0', 'amount1']));
		}

		console.log("[main] Loaded all events from cache");
	} else {
		console.log("[main] Fetching fresh data from HyperSync API...");

		// Step 1: Fetch Transfer events
		console.log("Querying Transfer events from NFT contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			NFT_CONTRACT,
			TRANSFER_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeTransferEvent(log);
				if (decoded) {
					allTransferEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allTransferEvents.length} Transfer events`);
		saveToCache('transfer', allTransferEvents);

		// Step 2: Fetch Mint events
		console.log("Querying Mint events from pool contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			CL_POOL,
			MINT_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeMintEvent(log);
				if (decoded) {
					allMintEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allMintEvents.length} Mint events`);
		saveToCache('mint', allMintEvents);

		// Step 3: Fetch Collect events
		console.log("Querying Collect events from pool contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			CL_POOL,
			COLLECT_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeCollectEvent(log);
				if (decoded) {
					allCollectEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allCollectEvents.length} Collect events`);
		saveToCache('collect', allCollectEvents);

		// Step 4: Fetch Burn events
		console.log("Querying Burn events from pool contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			CL_POOL,
			BURN_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeBurnEvent(log);
				if (decoded) {
					allBurnEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allBurnEvents.length} Burn events`);
		saveToCache('burn', allBurnEvents);

		// Step 5: Fetch IncreaseLiquidity events
		console.log("Querying IncreaseLiquidity events from NFT position manager contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			NFT_CONTRACT,
			INCREASE_LIQUIDITY_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeIncreaseLiquidityEvent(log);
				if (decoded) {
					allIncreaseLiquidityEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allIncreaseLiquidityEvents.length} IncreaseLiquidity events`);
		saveToCache('increaseLiquidity', allIncreaseLiquidityEvents);

		// Step 6: Fetch DecreaseLiquidity events
		console.log("Querying DecreaseLiquidity events from NFT position manager contract...");
		await fetchAndProcessLogs(
			HYPERSYNC_URL,
			NFT_CONTRACT,
			DECREASE_LIQUIDITY_EVENT_SIGNATURE,
			FROM_BLOCK,
			TO_BLOCK,
			(log) => {
				const decoded = decodeDecreaseLiquidityEvent(log);
				if (decoded) {
					allDecreaseLiquidityEvents.push(decoded);
				}
			}
		);
		console.log(`Fetched ${allDecreaseLiquidityEvents.length} DecreaseLiquidity events`);
		saveToCache('decreaseLiquidity', allDecreaseLiquidityEvents);

		console.log("[main] All data fetched and cached");
	}

	// ==================== DATA PROCESSING PHASE ====================
	console.log("[main] Processing events...");

	// Build tokenId -> ownership history mapping from Transfer events
	// Sort transfers by block number and log index to ensure correct order
	allTransferEvents.sort((a, b) => {
		if (a.blockNumber !== b.blockNumber) return a.blockNumber - b.blockNumber;
		return a.logIndex - b.logIndex;
	});

	// Map tokenId -> array of {blockNumber, owner} in chronological order
	const tokenOwnershipHistory = new Map<string, Array<{ blockNumber: number; owner: string }>>();
	// Also track original owner (first recipient from mint where from=0x0)
	const tokenOriginalOwner = new Map<string, string>();

	for (const transfer of allTransferEvents) {
		const tokenIdKey = transfer.tokenId.toString();

		// Track ownership history
		if (!tokenOwnershipHistory.has(tokenIdKey)) {
			tokenOwnershipHistory.set(tokenIdKey, []);
		}
		tokenOwnershipHistory.get(tokenIdKey)!.push({
			blockNumber: transfer.blockNumber,
			owner: transfer.to
		});

		// Track original owner (first recipient from mint)
		if (transfer.from === '0x0000000000000000000000000000000000000000' && !tokenOriginalOwner.has(tokenIdKey)) {
			tokenOriginalOwner.set(tokenIdKey, transfer.to);
		}
	}
	console.log(`Built token ownership history for ${tokenOwnershipHistory.size} unique token IDs`);
	console.log(`Tracked ${tokenOriginalOwner.size} original owners from mints`);

	// Helper function to get owner at a specific block
	const getOwnerAtBlock = (tokenId: string, blockNumber: number): string | null => {
		const history = tokenOwnershipHistory.get(tokenId);
		if (!history || history.length === 0) return null;

		// Find the last transfer that happened at or before this block
		let owner: string | null = null;
		for (const entry of history) {
			if (entry.blockNumber <= blockNumber) {
				owner = entry.owner;
			} else {
				break; // History is sorted, so we can stop here
			}
		}
		return owner;
	};

	// Known staking/wrapper contracts - if owner is one of these, use original owner instead
	const STAKING_CONTRACTS = new Set([
		'0x33732566dc8012bd3c9009ac05a85e6795e23098'.toLowerCase(), // Staking contract mentioned by user
	]);

	// Log memory usage warning if token map is very large
	if (tokenOwnershipHistory.size > 100000) {
		console.warn(`[main] Warning: Token ownership history has ${tokenOwnershipHistory.size} entries. This may use significant memory.`);
	}

	// Helper to resolve the beneficial owner (handles staking contracts)
	const getBeneficialOwner = (tokenId: string, blockNumber: number): string | null => {
		const ownerAtBlock = getOwnerAtBlock(tokenId, blockNumber);
		if (!ownerAtBlock) return null;

		// If owner is a staking contract or zero address, use original owner
		if (STAKING_CONTRACTS.has(ownerAtBlock.toLowerCase()) ||
			ownerAtBlock === '0x0000000000000000000000000000000000000000') {
			return tokenOriginalOwner.get(tokenId) || null;
		}

		return ownerAtBlock;
	};

	// Index Mint events by transaction hash (for matching with IncreaseLiquidity)
	console.log("Indexing Mint events by transaction...");
	const mintMapByTx = new Map<string, Array<{ mint: MintEvent; user: string; logIndex: number }>>();
	for (const mint of allMintEvents) {
		totalMintEvents++;
		const txHash = mint.transactionHash.toLowerCase();
		if (!mintMapByTx.has(txHash)) {
			mintMapByTx.set(txHash, []);
		}
		mintMapByTx.get(txHash)!.push({ mint, user: mint.owner, logIndex: mint.logIndex });
	}
	// Sort mint events by log index within each transaction
	for (const mints of mintMapByTx.values()) {
		mints.sort((a, b) => a.logIndex - b.logIndex);
	}
	console.log(`Indexed ${totalMintEvents} Mint events from pool`);

	// Index Collect events by transaction hash
	console.log("Indexing Collect events by transaction...");
	const collectMapByTx = new Map<string, Array<CollectEvent>>();
	for (const collect of allCollectEvents) {
		totalCollectEvents++;
		const txHash = collect.transactionHash.toLowerCase();
		if (!collectMapByTx.has(txHash)) {
			collectMapByTx.set(txHash, []);
		}
		collectMapByTx.get(txHash)!.push(collect);
	}
	// Sort collect events by log index within each transaction
	for (const collects of collectMapByTx.values()) {
		collects.sort((a, b) => a.logIndex - b.logIndex);
	}
	console.log(`Indexed ${totalCollectEvents} Collect events`);

	// Track which collect events are matched with DecreaseLiquidity later
	const matchedCollectTxHashes = new Set<string>();

	// Index Burn events by transaction hash
	console.log("Indexing Burn events by transaction...");
	const burnMapByTx = new Map<string, Array<BurnEvent>>();
	for (const burn of allBurnEvents) {
		totalBurnEvents++;
		const txHash = burn.transactionHash.toLowerCase();
		if (!burnMapByTx.has(txHash)) {
			burnMapByTx.set(txHash, []);
		}
		burnMapByTx.get(txHash)!.push(burn);
	}
	// Sort burn events by log index within each transaction
	for (const burns of burnMapByTx.values()) {
		burns.sort((a, b) => a.logIndex - b.logIndex);
	}
	console.log(`Indexed ${totalBurnEvents} Burn events`);

	// Index IncreaseLiquidity events by transaction hash and match with Mint events
	console.log("Indexing and matching IncreaseLiquidity events...");
	const increaseLiquidityMapByTx = new Map<string, Array<IncreaseLiquidityEvent>>();
	for (const il of allIncreaseLiquidityEvents) {
		totalIncreaseLiquidityEvents++;
		const txHash = il.transactionHash.toLowerCase();
		if (!increaseLiquidityMapByTx.has(txHash)) {
			increaseLiquidityMapByTx.set(txHash, []);
		}
		increaseLiquidityMapByTx.get(txHash)!.push(il);
	}
	// Sort IncreaseLiquidity events by log index within each transaction and match with Mint events
	for (const [txHash, increaseLiquidityEvents] of increaseLiquidityMapByTx.entries()) {
		increaseLiquidityEvents.sort((a, b) => a.logIndex - b.logIndex);
		const mintEvents = mintMapByTx.get(txHash) || [];

		// Match each IncreaseLiquidity with the closest Mint event by log index
		// (They should be very close since they're part of the same operation)
		for (const increaseLiquidity of increaseLiquidityEvents) {
			// Find the Mint with the closest log index (prefer one that comes before IncreaseLiquidity)
			let bestMatch: { mint: MintEvent; user: string; logIndex: number } | null = null;
			let bestDistance = Infinity;

			for (const mintEvent of mintEvents) {
				const distance = Math.abs(mintEvent.logIndex - increaseLiquidity.logIndex);
				// Prefer mints that come just before the IncreaseLiquidity (they're part of same operation)
				if (mintEvent.logIndex <= increaseLiquidity.logIndex && distance < bestDistance) {
					bestMatch = mintEvent;
					bestDistance = distance;
				}
			}

			// If no mint found before, try finding any nearby mint
			if (!bestMatch) {
				for (const mintEvent of mintEvents) {
					const distance = Math.abs(mintEvent.logIndex - increaseLiquidity.logIndex);
					if (distance < bestDistance) {
						bestMatch = mintEvent;
						bestDistance = distance;
					}
				}
			}

			if (bestMatch && bestDistance < 10) { // Only match if log indices are within 10 of each other
				// Look up the beneficial owner at the time of this event
				const tokenIdKey = increaseLiquidity.tokenId.toString();
				const beneficialOwner = getBeneficialOwner(tokenIdKey, increaseLiquidity.blockNumber);

				if (!beneficialOwner) {
					console.warn(`[main] IncreaseLiquidity event for tokenId ${tokenIdKey} has no owner in transfer map - position may have been created before FROM_BLOCK`);
					continue;
				}

				// Write IncreaseLiquidity event as CSV row with beneficial owner
				const row = [
					'increaseLiquidity',
					increaseLiquidity.transactionHash,
					increaseLiquidity.blockNumber.toString(),
					increaseLiquidity.liquidity.toString(),
					increaseLiquidity.amount0.toString(),
					increaseLiquidity.amount1.toString(),
					beneficialOwner,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedIncreaseLiquidityCount++;
			} else {
				console.warn(`[main] IncreaseLiquidity event at tx ${increaseLiquidity.transactionHash} (logIndex ${increaseLiquidity.logIndex}) has no matching Mint event nearby (best distance: ${bestDistance})`);
			}
		}
	}
	console.log(`Processed ${totalIncreaseLiquidityEvents} IncreaseLiquidity events (${matchedIncreaseLiquidityCount} matched)`);

	// Index DecreaseLiquidity events by transaction hash and match with Burn events
	console.log("Indexing and matching DecreaseLiquidity events...");
	const decreaseLiquidityMapByTx = new Map<string, Array<DecreaseLiquidityEvent>>();
	for (const dl of allDecreaseLiquidityEvents) {
		totalDecreaseLiquidityEvents++;
		const txHash = dl.transactionHash.toLowerCase();
		if (!decreaseLiquidityMapByTx.has(txHash)) {
			decreaseLiquidityMapByTx.set(txHash, []);
		}
		decreaseLiquidityMapByTx.get(txHash)!.push(dl);
	}
	// Sort DecreaseLiquidity events by log index within each transaction and match with Burn events
	for (const [txHash, decreaseLiquidityEvents] of decreaseLiquidityMapByTx.entries()) {
		decreaseLiquidityEvents.sort((a, b) => a.logIndex - b.logIndex);
		const burnEvents = burnMapByTx.get(txHash) || [];
		const collectEvents = collectMapByTx.get(txHash) || [];

		// Match each DecreaseLiquidity with the closest Burn event by log index
		for (const decreaseLiquidity of decreaseLiquidityEvents) {
			// Find the Burn with the closest log index (prefer one that comes after DecreaseLiquidity)
			let bestBurnMatch: BurnEvent | null = null;
			let bestBurnDistance = Infinity;

			for (const burnEvent of burnEvents) {
				const distance = Math.abs(burnEvent.logIndex - decreaseLiquidity.logIndex);
				// Prefer burns that come just after the DecreaseLiquidity (they're part of same operation)
				if (burnEvent.logIndex >= decreaseLiquidity.logIndex && distance < bestBurnDistance) {
					bestBurnMatch = burnEvent;
					bestBurnDistance = distance;
				}
			}

			// If no burn found after, try finding any nearby burn
			if (!bestBurnMatch) {
				for (const burnEvent of burnEvents) {
					const distance = Math.abs(burnEvent.logIndex - decreaseLiquidity.logIndex);
					if (distance < bestBurnDistance) {
						bestBurnMatch = burnEvent;
						bestBurnDistance = distance;
					}
				}
			}

			if (bestBurnMatch && bestBurnDistance < 10) { // Only match if log indices are within 10 of each other
				// Mark this collect as matched with decreaseLiquidity
				matchedCollectTxHashes.add(txHash);

				// Look up the beneficial owner at the time of this event
				const tokenIdKey = decreaseLiquidity.tokenId.toString();
				const beneficialOwner = getBeneficialOwner(tokenIdKey, decreaseLiquidity.blockNumber);

				if (!beneficialOwner) {
					console.warn(`[main] DecreaseLiquidity event for tokenId ${tokenIdKey} has no owner in transfer map - position may have been created before FROM_BLOCK`);
					continue;
				}

				// Write DecreaseLiquidity event as CSV row with beneficial owner
				const row = [
					'decreaseLiquidity',
					decreaseLiquidity.transactionHash,
					decreaseLiquidity.blockNumber.toString(),
					decreaseLiquidity.liquidity.toString(),
					decreaseLiquidity.amount0.toString(),
					decreaseLiquidity.amount1.toString(),
					beneficialOwner,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					// Handle backpressure - wait for drain event
					outputFile.once('drain', () => {});
				}
				matchedDecreaseLiquidityCount++;
			} else {
				console.warn(`[main] DecreaseLiquidity event at tx ${decreaseLiquidity.transactionHash} (logIndex ${decreaseLiquidity.logIndex}) has no matching Burn event nearby (best distance: ${bestBurnDistance})`);
			}
		}
	}
	console.log(`Processed ${totalDecreaseLiquidityEvents} DecreaseLiquidity events (${matchedDecreaseLiquidityCount} matched)`);

	// Step 6: Write standalone Collect events (fee collection without liquidity removal)
	console.log("Processing standalone Collect events (fee collection)...");
	let standaloneCollectCount = 0;
	for (const [txHash, collects] of collectMapByTx.entries()) {
		// If this tx has no DecreaseLiquidity, it's a standalone fee collection
		if (!matchedCollectTxHashes.has(txHash)) {
			for (const collect of collects) {
				const row = [
					'collect',
					collect.transactionHash,
					collect.blockNumber.toString(),
					'0', // No liquidity change
					collect.amount0.toString(),
					collect.amount1.toString(),
					collect.recipient,
				];
				const csvRow = row.map(escapeCsvValue).join(',') + '\n';
				if (!outputFile.write(csvRow)) {
					outputFile.once('drain', () => {});
				}
				standaloneCollectCount++;
			}
		}
	}
	console.log(`Processed ${standaloneCollectCount} standalone Collect events (fee collection)`);

	// Close the file
	outputFile.end();

	// Wait for file to finish writing
	await new Promise((resolve) => outputFile.on('finish', resolve));

	console.log(`[main] Results written to ${outputPath}`);
	console.log(`Total events written to CSV:`);
	console.log(`  - ${matchedIncreaseLiquidityCount} increaseLiquidity events (liquidity added)`);
	console.log(`  - ${matchedDecreaseLiquidityCount} decreaseLiquidity events (liquidity removed)`);
	console.log(`  - ${standaloneCollectCount} collect events (fee collection only)`);
	console.log(`Supporting events (used for matching, not separate CSV rows):`);
	console.log(`  - ${totalMintEvents} mint events, ${totalBurnEvents} burn events, ${totalCollectEvents} collect events`);
	console.log("[main] Main function completed successfully");
}

console.log("[startup] Script starting...");
main().catch((error) => {
	console.error("[startup] Fatal error in main:", error);
	process.exit(1);
});
